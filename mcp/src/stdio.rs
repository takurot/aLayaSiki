use crate::{protocol::error, Connection, MAX_MESSAGE_BYTES};
use serde_json::Value;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

/// Reads without allocating beyond the bounded wire frame. Oversized frames
/// are discarded through their newline before accepting the next message.
pub async fn run<R: AsyncBufRead + Unpin, W: AsyncWrite + Unpin>(
    mut input: R,
    mut output: W,
    mut connection: Connection,
) -> std::io::Result<()> {
    loop {
        let mut frame = Vec::new();
        let mut oversized = false;
        let mut read_any = false;
        loop {
            let bytes = input.fill_buf().await?;
            if bytes.is_empty() {
                break;
            }
            read_any = true;
            let consumed = bytes
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(bytes.len(), |end| end + 1);
            if frame.len() + consumed > MAX_MESSAGE_BYTES {
                oversized = true;
            } else if !oversized {
                frame.extend_from_slice(&bytes[..consumed]);
            }
            let newline = bytes[consumed - 1] == b'\n';
            input.consume(consumed);
            if newline {
                break;
            }
        }
        if !read_any {
            break;
        }
        let response = if oversized {
            Some(error(Value::Null, -32600, "Message too large"))
        } else {
            match serde_json::from_slice(&frame) {
                Ok(value) => connection.dispatch(value).await,
                Err(_) => Some(error(Value::Null, -32700, "Parse error")),
            }
        };
        if let Some(response) = response {
            let mut bytes = serde_json::to_vec(&response)?;
            if bytes.len() > MAX_MESSAGE_BYTES {
                bytes = serde_json::to_vec(&error(
                    response["id"].clone(),
                    -32603,
                    "Response too large",
                ))?
            }
            output.write_all(&bytes).await?;
            output.write_all(b"\n").await?;
            output.flush().await?;
        }
    }
    Ok(())
}
