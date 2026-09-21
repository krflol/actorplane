use std::{
    io::ErrorKind,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    Io(ErrorKind),
    TooLarge,
    BudgetExceeded,
    AllocationFailed,
    InvalidConfig,
    Truncated,
}

struct BudgetState {
    limit: usize,
    used: AtomicUsize,
}

#[derive(Clone)]
pub struct BufferBudget {
    state: Arc<BudgetState>,
    native: Option<actorplane_core::NativeBufferBudget>,
}

impl BufferBudget {
    pub fn new(limit: usize) -> Result<Self, FrameError> {
        Ok(Self {
            state: Arc::new(BudgetState {
                limit,
                used: AtomicUsize::new(0),
            }),
            native: None,
        })
    }

    pub fn used(&self) -> usize {
        self.state.used.load(Ordering::Acquire)
    }

    pub(crate) fn with_world(
        limit: usize,
        world: &actorplane_core::World,
    ) -> Result<Self, FrameError> {
        let mut budget = Self::new(limit)?;
        budget.native = Some(world.native_buffer_budget());
        Ok(budget)
    }

    pub(crate) fn reserve(&self, bytes: usize) -> Result<BufferPermit, FrameError> {
        let mut permit = self.reserve_local(bytes)?;
        if let Some(native) = &self.native {
            permit.native = Some(
                native
                    .reserve(bytes)
                    .map_err(|_| FrameError::BudgetExceeded)?,
            );
        }
        Ok(permit)
    }

    fn reserve_local(&self, bytes: usize) -> Result<BufferPermit, FrameError> {
        let mut current = self.state.used.load(Ordering::Acquire);
        loop {
            let next = current
                .checked_add(bytes)
                .ok_or(FrameError::BudgetExceeded)?;
            if next > self.state.limit {
                return Err(FrameError::BudgetExceeded);
            }
            match self.state.used.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(BufferPermit {
                        budget: self.clone(),
                        charged: bytes,
                        native: None,
                    });
                }
                Err(value) => current = value,
            }
        }
    }

    fn release(&self, bytes: usize) {
        self.state.used.fetch_sub(bytes, Ordering::AcqRel);
    }
}

pub(crate) struct BufferPermit {
    budget: BufferBudget,
    charged: usize,
    native: Option<actorplane_core::NativeBufferPermit>,
}

impl BufferPermit {
    fn grow(&mut self, bytes: usize) -> Result<(), FrameError> {
        if bytes == 0 {
            return Ok(());
        }
        let charged = self
            .charged
            .checked_add(bytes)
            .ok_or(FrameError::BudgetExceeded)?;
        let mut extra = self.budget.reserve_local(bytes)?;
        if let Some(native) = &mut self.native {
            native
                .try_grow(bytes)
                .map_err(|_| FrameError::BudgetExceeded)?;
        }
        self.charged = charged;
        // Transfer the charge to this permit while allowing the temporary
        // permit to drop normally (and release no bytes).
        extra.charged = 0;
        Ok(())
    }
}

impl Drop for BufferPermit {
    fn drop(&mut self) {
        self.budget.release(self.charged);
    }
}

pub struct FrameBuffer {
    bytes: Vec<u8>,
    _permit: BufferPermit,
}

impl FrameBuffer {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

fn io_error(error: std::io::Error) -> FrameError {
    FrameError::Io(error.kind())
}

async fn read_exact_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    bytes: &mut [u8],
) -> Result<bool, FrameError> {
    let mut offset = 0;
    while offset < bytes.len() {
        let read = reader.read(&mut bytes[offset..]).await.map_err(io_error)?;
        if read == 0 {
            if offset == 0 {
                return Ok(false);
            }
            return Err(FrameError::Truncated);
        }
        offset += read;
    }
    Ok(true)
}

pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    budget: &BufferBudget,
    max_frame_bytes: usize,
) -> Result<Option<FrameBuffer>, FrameError> {
    let mut header = [0u8; 4];
    if !read_exact_frame(reader, &mut header).await? {
        return Ok(None);
    }
    let length = u32::from_be_bytes(header) as usize;
    if length > max_frame_bytes {
        return Err(FrameError::TooLarge);
    }
    if length > isize::MAX as usize {
        return Err(FrameError::InvalidConfig);
    }
    let mut permit = budget.reserve(length)?;
    let mut bytes = Vec::new();
    if bytes.try_reserve_exact(length).is_err() {
        return Err(FrameError::AllocationFailed);
    }
    let actual = bytes.capacity();
    if actual > length {
        permit.grow(actual - length)?;
    }
    bytes.resize(length, 0);
    if !read_exact_frame(reader, &mut bytes).await? {
        return Err(FrameError::Truncated);
    }
    Ok(Some(FrameBuffer {
        bytes,
        _permit: permit,
    }))
}

pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    payload: &[u8],
    max_frame_bytes: usize,
) -> Result<(), FrameError> {
    if payload.len() > max_frame_bytes || payload.len() > u32::MAX as usize {
        return Err(FrameError::TooLarge);
    }
    writer
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await
        .map_err(io_error)?;
    writer.write_all(payload).await.map_err(io_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::io::AsyncWrite;
    use tokio::io::duplex;

    struct ChunkWriter {
        bytes: Vec<u8>,
        chunk: usize,
    }

    impl AsyncWrite for ChunkWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            input: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let count = input.len().min(self.chunk);
            self.bytes.extend_from_slice(&input[..count]);
            Poll::Ready(Ok(count))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn fragmented_and_coalesced_frames_round_trip() {
        let budget = BufferBudget::new(32).unwrap();
        let (mut writer, mut reader) = duplex(64);
        writer.write_all(&[0, 0]).await.unwrap();
        writer.write_all(&[0, 3, b'a', b'b']).await.unwrap();
        writer.write_all(&[b'c', 0, 0, 0, 0]).await.unwrap();
        drop(writer);
        assert_eq!(
            read_frame(&mut reader, &budget, 16)
                .await
                .unwrap()
                .unwrap()
                .bytes(),
            b"abc"
        );
        assert_eq!(
            read_frame(&mut reader, &budget, 16)
                .await
                .unwrap()
                .unwrap()
                .bytes(),
            b""
        );
        assert!(
            read_frame(&mut reader, &budget, 16)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn header_without_body_is_truncated() {
        let budget = BufferBudget::new(8).unwrap();
        let (mut writer, mut reader) = duplex(16);
        writer.write_all(&[0, 0, 0, 4]).await.unwrap();
        drop(writer);
        assert!(matches!(
            read_frame(&mut reader, &budget, 8).await,
            Err(FrameError::Truncated)
        ));
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn oversize_and_truncated_frames_do_not_charge_budget() {
        let budget = BufferBudget::new(8).unwrap();
        let (mut writer, mut reader) = duplex(32);
        writer.write_all(&[0, 0, 0, 9]).await.unwrap();
        assert!(matches!(
            read_frame(&mut reader, &budget, 8).await,
            Err(FrameError::TooLarge)
        ));
        writer.write_all(&[0, 0, 0, 4, 1, 2]).await.unwrap();
        drop(writer);
        assert!(matches!(
            read_frame(&mut reader, &budget, 8).await,
            Err(FrameError::Truncated)
        ));
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn shared_budget_and_writer_are_bounded() {
        let budget = BufferBudget::new(3).unwrap();
        let (mut writer, mut reader) = duplex(32);
        write_frame(&mut writer, b"abc", 3).await.unwrap();
        writer.shutdown().await.unwrap();
        let frame = read_frame(&mut reader, &budget, 3).await.unwrap().unwrap();
        assert_eq!(budget.used(), 3);
        assert!(read_frame(&mut reader, &budget, 3).await.unwrap().is_none());
        drop(frame);
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn cancelled_read_releases_reserved_body() {
        let budget = BufferBudget::new(16).unwrap();
        let (mut writer, reader) = duplex(16);
        writer.write_all(&[0, 0, 0, 4]).await.unwrap();
        let task_budget = budget.clone();
        let task = tokio::spawn(async move {
            let mut reader = reader;
            read_frame(&mut reader, &task_budget, 16).await
        });

        for _ in 0..32 {
            if budget.used() == 4 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(budget.used(), 4);
        task.abort();
        let _ = task.await;
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn writer_handles_partial_async_writes() {
        let mut writer = ChunkWriter {
            bytes: Vec::new(),
            chunk: 2,
        };
        write_frame(&mut writer, b"abcdef", 16).await.unwrap();
        assert_eq!(
            writer.bytes,
            [0, 0, 0, 6, b'a', b'b', b'c', b'd', b'e', b'f']
        );
    }
}
