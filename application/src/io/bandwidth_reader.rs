use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, ReadBuf};

pub struct BandwidthReader<R> {
    inner: R,
    server: crate::server::Server,
    block_at_quota: bool,
    count_traffic: bool,
}

impl<R> BandwidthReader<R> {
    pub fn new(inner: R, server: crate::server::Server) -> Self {
        Self {
            inner,
            server,
            block_at_quota: true,
            count_traffic: true,
        }
    }

    pub fn new_administrative(inner: R, server: crate::server::Server) -> Self {
        Self {
            inner,
            server,
            block_at_quota: false,
            count_traffic: false,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for BandwidthReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.block_at_quota && self.server.bandwidth.blocked() {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "bandwidth quota reached",
            )));
        }
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if self.count_traffic
            && let Poll::Ready(Ok(())) = &result
        {
            let bytes = (buf.filled().len() - before) as u64;
            self.server.bandwidth.record_stream(0, bytes);
        }
        result
    }
}
