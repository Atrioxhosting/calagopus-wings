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

impl<R: std::io::Read> std::io::Read for BandwidthReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.block_at_quota && self.server.bandwidth.blocked() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "bandwidth quota reached",
            ));
        }
        let bytes = self.inner.read(buf)?;
        if self.count_traffic {
            self.server.bandwidth.record_stream(0, bytes as u64);
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::BandwidthReader;
    use futures::TryStreamExt;
    use std::io::{Cursor, Read};

    #[tokio::test]
    async fn fast_download_stream_preserves_accounting_and_quota() {
        let (_temp, server) = crate::server::Server::mock_in_tempdir().await;
        let payload = b"Atriox fast download";
        let before = server.bandwidth.snapshot().tx_bytes;
        let reader = BandwidthReader::new(Cursor::new(payload), server.clone());
        let chunks: Vec<bytes::Bytes> =
            crate::io::read_stream::ReadStream::new(reader, payload.len() as u64, 4)
                .try_collect()
                .await
                .unwrap();
        assert_eq!(chunks.concat(), payload);
        assert_eq!(
            server.bandwidth.snapshot().tx_bytes - before,
            payload.len() as u64
        );

        // A truncated file still sends the advertised length, including padding.
        let before = server.bandwidth.snapshot().tx_bytes;
        let reader = BandwidthReader::new(
            crate::io::fixed_reader::FixedReader::new_with_fixed_bytes(Cursor::new(b"x"), 4),
            server.clone(),
        );
        let chunks: Vec<bytes::Bytes> = crate::io::read_stream::ReadStream::new(reader, 4, 4)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(chunks.concat(), b"x\0\0\0");
        assert_eq!(server.bandwidth.snapshot().tx_bytes - before, 4);

        server.bandwidth.evaluate(Some(1), false);
        assert!(server.bandwidth.blocked());
        let mut reader = BandwidthReader::new(Cursor::new(payload), server.clone());
        let mut buffer = [0; 4];
        assert_eq!(
            reader.read(&mut buffer).unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );

        let before = server.bandwidth.snapshot().tx_bytes;
        let mut reader = BandwidthReader::new_administrative(Cursor::new(payload), server.clone());
        assert_eq!(reader.read(&mut buffer).unwrap(), buffer.len());
        assert_eq!(server.bandwidth.snapshot().tx_bytes, before);
    }
}
