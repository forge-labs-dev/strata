//! Rust acceleration for Strata's data plane.
//!
//! This module provides fast Arrow IPC read/write operations,
//! eliminating Python from the hot path for cache hits.
//!
//! Two approaches:
//! 1. Fast path: Low-level byte manipulation to convert between IPC File and Stream
//!    formats without deserializing Arrow data (much faster for cached data)
//! 2. Fallback: Full Arrow parsing when low-level manipulation isn't possible
//!
//! Arrow IPC File format:  ARROW1 + schema + [record batches] + footer
//! Arrow IPC Stream format: schema + [record batches] + EOS marker

use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::StreamWriter;
use memmap2::Mmap;
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::fs::File;
use std::io::Cursor;
use thiserror::Error;

// Arrow IPC constants
const CONTINUATION_MARKER: [u8; 4] = [0xFF, 0xFF, 0xFF, 0xFF];
const EOS_MARKER: [u8; 8] = [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00];

#[derive(Error, Debug)]
pub enum StrataError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Invalid file: {0}")]
    InvalidFile(String),
}

impl From<StrataError> for PyErr {
    fn from(err: StrataError) -> PyErr {
        match err {
            StrataError::Io(e) => PyIOError::new_err(e.to_string()),
            StrataError::Arrow(e) => PyValueError::new_err(e.to_string()),
            StrataError::InvalidFile(msg) => PyValueError::new_err(msg),
        }
    }
}

/// Read an Arrow IPC file from disk and return it as IPC stream bytes.
///
/// This is the core optimization: we memory-map the file, parse the Arrow
/// schema, and immediately re-serialize to IPC stream format for network
/// transfer. No Python object creation for the actual data.
///
/// Args:
///     path: Path to the Arrow IPC file (.arrowfile)
///
/// Returns:
///     bytes: Arrow IPC stream format bytes ready for network transfer
#[pyfunction]
fn read_arrow_ipc_as_stream<'py>(py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyBytes>> {
    // Memory-map the file for zero-copy read
    let file = File::open(path).map_err(StrataError::from)?;
    let mmap = unsafe { Mmap::map(&file) }.map_err(StrataError::from)?;

    // Parse as Arrow IPC File format using the Arrow library
    let cursor = Cursor::new(&mmap[..]);
    let reader = FileReader::try_new(cursor, None).map_err(StrataError::from)?;

    let schema = reader.schema();

    // Pre-allocate buffer (estimate: same size as input)
    let mut buffer = Vec::with_capacity(mmap.len());

    // Write as IPC stream format
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema).map_err(StrataError::from)?;

        for batch_result in reader {
            let batch = batch_result.map_err(StrataError::from)?;
            writer.write(&batch).map_err(StrataError::from)?;
        }
        writer.finish().map_err(StrataError::from)?;
    }

    // Return as Python bytes
    Ok(PyBytes::new_bound(py, &buffer))
}

/// Read an Arrow IPC file and return raw bytes (for cache passthrough).
///
/// Even simpler: just read the file bytes. Python can decide whether
/// to parse or pass through.
///
/// Args:
///     path: Path to the Arrow IPC file
///
/// Returns:
///     bytes: Raw file contents
#[pyfunction]
fn read_file_bytes<'py>(py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyBytes>> {
    let file = File::open(path).map_err(StrataError::from)?;
    let mmap = unsafe { Mmap::map(&file) }.map_err(StrataError::from)?;
    Ok(PyBytes::new_bound(py, &mmap[..]))
}

/// Convert Arrow IPC file format to stream format.
///
/// Takes file format bytes (what's on disk) and returns stream format
/// bytes (what goes on the network). This is useful when you already
/// have the bytes in memory.
///
/// Args:
///     data: Arrow IPC file format bytes
///
/// Returns:
///     bytes: Arrow IPC stream format bytes
#[pyfunction]
fn file_to_stream_format<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    let cursor = Cursor::new(data);
    let reader = FileReader::try_new(cursor, None).map_err(StrataError::from)?;

    let schema = reader.schema();
    let mut buffer = Vec::with_capacity(data.len());

    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema).map_err(StrataError::from)?;

        for batch_result in reader {
            let batch = batch_result.map_err(StrataError::from)?;
            writer.write(&batch).map_err(StrataError::from)?;
        }
        writer.finish().map_err(StrataError::from)?;
    }

    Ok(PyBytes::new_bound(py, &buffer))
}

/// Fast concatenation of Arrow IPC streams by byte manipulation.
///
/// Arrow IPC Stream format:
/// - Schema message (continuation + size + flatbuffer)
/// - Record batch messages (continuation + size + flatbuffer + data)
/// - EOS marker (0xFFFFFFFF 0x00000000)
///
/// To concatenate streams: take schema from first, strip schema from rest,
/// combine all record batches, add single EOS.
fn concat_streams_fast(segments: &[Vec<u8>]) -> Result<Vec<u8>, StrataError> {
    if segments.is_empty() {
        return Ok(Vec::new());
    }

    if segments.len() == 1 {
        return Ok(segments[0].clone());
    }

    // Estimate total size
    let total_size: usize = segments.iter().map(|s| s.len()).sum();
    let mut result = Vec::with_capacity(total_size);

    // Copy first segment completely (includes schema and EOS)
    // But we need to strip the EOS marker (last 8 bytes)
    let first = &segments[0];
    if first.len() < 8 {
        return Err(StrataError::InvalidFile("First segment too small".into()));
    }

    // Verify EOS marker at end of first segment
    if &first[first.len() - 8..] != &EOS_MARKER {
        return Err(StrataError::InvalidFile("First segment missing EOS marker".into()));
    }

    // Copy first segment without EOS
    result.extend_from_slice(&first[..first.len() - 8]);

    // For subsequent segments, we need to skip the schema message and copy only record batches
    for segment in &segments[1..] {
        if segment.len() < 8 {
            continue;
        }

        // Verify EOS marker
        if &segment[segment.len() - 8..] != &EOS_MARKER {
            return Err(StrataError::InvalidFile("Segment missing EOS marker".into()));
        }

        // Find where record batches start by parsing message headers
        // Schema message format: continuation (4) + size (4) + flatbuffer (size bytes)
        let mut offset = 0;

        // Skip continuation marker if present
        if segment.len() >= 4 && &segment[0..4] == &CONTINUATION_MARKER {
            offset = 4;
        }

        // Read schema message size
        if offset + 4 > segment.len() {
            continue;
        }
        let schema_size = u32::from_le_bytes([
            segment[offset],
            segment[offset + 1],
            segment[offset + 2],
            segment[offset + 3],
        ]) as usize;
        offset += 4 + schema_size;

        // Align to 8 bytes
        offset = (offset + 7) & !7;

        // Copy record batches (everything from offset to len-8)
        if offset < segment.len() - 8 {
            result.extend_from_slice(&segment[offset..segment.len() - 8]);
        }
    }

    // Add final EOS marker
    result.extend_from_slice(&EOS_MARKER);

    Ok(result)
}

/// Fast concatenation of Arrow IPC streams using a pre-concatenated buffer.
///
/// This avoids the PyO3 Vec<Vec<u8>> copy overhead by accepting a single
/// buffer with offset information.
///
/// Args:
///     data: Pre-concatenated bytes of all segments
///     offsets: List of (start, end) tuples for each segment
///
/// Returns:
///     bytes: Single combined Arrow IPC stream
#[pyfunction]
fn concat_ipc_streams_fast<'py>(
    py: Python<'py>,
    data: &[u8],
    offsets: Vec<(usize, usize)>,
) -> PyResult<Bound<'py, PyBytes>> {
    if offsets.is_empty() {
        return Ok(PyBytes::new_bound(py, &[]));
    }

    if offsets.len() == 1 {
        let (start, end) = offsets[0];
        return Ok(PyBytes::new_bound(py, &data[start..end]));
    }

    // Estimate output size (slightly less than input due to removed schemas)
    let mut result = Vec::with_capacity(data.len());

    // Process first segment - keep everything except EOS marker
    let (start, end) = offsets[0];
    let first = &data[start..end];

    if first.len() < 8 {
        return Err(StrataError::InvalidFile("First segment too small".into()).into());
    }

    // Verify EOS marker
    if &first[first.len() - 8..] != &EOS_MARKER {
        return Err(StrataError::InvalidFile("First segment missing EOS marker".into()).into());
    }

    // Copy first segment without EOS
    result.extend_from_slice(&first[..first.len() - 8]);

    // Process remaining segments - skip schema, keep record batches
    for &(start, end) in &offsets[1..] {
        let segment = &data[start..end];

        if segment.len() < 8 {
            continue;
        }

        // Verify EOS marker
        if &segment[segment.len() - 8..] != &EOS_MARKER {
            return Err(StrataError::InvalidFile("Segment missing EOS marker".into()).into());
        }

        // Find where record batches start
        let mut offset = 0;

        // Skip continuation marker if present
        if segment.len() >= 4 && &segment[0..4] == &CONTINUATION_MARKER {
            offset = 4;
        }

        // Read schema message size
        if offset + 4 > segment.len() {
            continue;
        }
        let schema_size = u32::from_le_bytes([
            segment[offset],
            segment[offset + 1],
            segment[offset + 2],
            segment[offset + 3],
        ]) as usize;
        offset += 4 + schema_size;

        // Align to 8 bytes
        offset = (offset + 7) & !7;

        // Copy record batches (everything from offset to len-8)
        if offset < segment.len() - 8 {
            result.extend_from_slice(&segment[offset..segment.len() - 8]);
        }
    }

    // Add final EOS marker
    result.extend_from_slice(&EOS_MARKER);

    Ok(PyBytes::new_bound(py, &result))
}

/// Concatenate multiple Arrow IPC stream segments into one.
///
/// When serving multiple row groups, we need to combine them into
/// a single IPC stream. This does it efficiently in Rust using
/// byte manipulation rather than full Arrow parsing.
///
/// Args:
///     segments: List of Arrow IPC stream bytes
///
/// Returns:
///     bytes: Single combined Arrow IPC stream
#[pyfunction]
fn concat_ipc_streams<'py>(py: Python<'py>, segments: Vec<Vec<u8>>) -> PyResult<Bound<'py, PyBytes>> {
    // Try fast path first (byte manipulation)
    match concat_streams_fast(&segments) {
        Ok(result) => return Ok(PyBytes::new_bound(py, &result)),
        Err(_) => {
            // Fall back to full Arrow parsing
        }
    }

    // Fallback: Full Arrow parsing (slower but handles edge cases)
    use arrow::ipc::reader::StreamReader;

    if segments.is_empty() {
        return Ok(PyBytes::new_bound(py, &[]));
    }

    // Read first segment to get schema
    let first_cursor = Cursor::new(&segments[0]);
    let first_reader = StreamReader::try_new(first_cursor, None).map_err(StrataError::from)?;
    let schema = first_reader.schema();

    // Collect all batches
    let mut all_batches = Vec::new();

    for segment in &segments {
        let cursor = Cursor::new(segment);
        let reader = StreamReader::try_new(cursor, None).map_err(StrataError::from)?;

        for batch_result in reader {
            let batch = batch_result.map_err(StrataError::from)?;
            all_batches.push(batch);
        }
    }

    // Write combined stream
    let estimated_size: usize = segments.iter().map(|s| s.len()).sum();
    let mut buffer = Vec::with_capacity(estimated_size);

    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema).map_err(StrataError::from)?;

        for batch in all_batches {
            writer.write(&batch).map_err(StrataError::from)?;
        }
        writer.finish().map_err(StrataError::from)?;
    }

    Ok(PyBytes::new_bound(py, &buffer))
}

/// Get statistics about an Arrow IPC file without fully parsing it.
///
/// Returns (num_batches, total_rows, schema_json) for quick inspection.
#[pyfunction]
fn ipc_file_stats(path: &str) -> PyResult<(usize, usize, String)> {
    let file = File::open(path).map_err(StrataError::from)?;
    let mmap = unsafe { Mmap::map(&file) }.map_err(StrataError::from)?;

    let cursor = Cursor::new(&mmap[..]);
    let reader = FileReader::try_new(cursor, None).map_err(StrataError::from)?;

    let schema = reader.schema();
    let num_batches = reader.num_batches();

    // Count total rows
    let mut total_rows = 0;
    for batch_result in reader {
        let batch = batch_result.map_err(StrataError::from)?;
        total_rows += batch.num_rows();
    }

    // Simple schema representation
    let schema_str = format!("{:?}", schema);

    Ok((num_batches, total_rows, schema_str))
}

/// Python module definition
#[pymodule]
#[pyo3(name = "_strata_core")]
fn strata_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_arrow_ipc_as_stream, m)?)?;
    m.add_function(wrap_pyfunction!(read_file_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(file_to_stream_format, m)?)?;
    m.add_function(wrap_pyfunction!(concat_ipc_streams, m)?)?;
    m.add_function(wrap_pyfunction!(concat_ipc_streams_fast, m)?)?;
    m.add_function(wrap_pyfunction!(ipc_file_stats, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;
    use std::io::Write;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    fn create_test_ipc_file() -> NamedTempFile {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();

        let mut file = NamedTempFile::new().unwrap();
        {
            let mut writer = FileWriter::try_new(file.as_file_mut(), &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        file.as_file_mut().flush().unwrap();
        file
    }

    #[test]
    fn test_ipc_file_stats() {
        let file = create_test_ipc_file();
        let path = file.path().to_str().unwrap();

        let (num_batches, total_rows, _schema) = ipc_file_stats(path).unwrap();
        assert_eq!(num_batches, 1);
        assert_eq!(total_rows, 5);
    }
}
