pub mod bench_eval;

use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[rkyv(attr(repr(C)))]
pub struct Node {
    pub id: u64,
    pub embedding: Vec<f32>,
    pub metadata: String, // Simulating JSON for now
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[rkyv(attr(repr(C)))]
pub struct Edge {
    pub source: u64,
    pub target: u64,
    pub relation_type: u8,
    pub weight: f32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rkyv_zero_copy() {
        let node = Node {
            id: 1,
            embedding: vec![0.1, 0.2, 0.3],
            metadata: "{\"name\": \"Alice\"}".to_string(),
        };

        // Serialize
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&node).expect("failed to serialize");

        // Deserialize (Zero-copy access with validation)
        // rkyv::access verifies the archive's integrity without full deserialization
        let archived = rkyv::access::<ArchivedNode, rkyv::rancor::Error>(&bytes[..])
            .expect("failed to verify archive");

        assert_eq!(archived.id, 1);
        assert_eq!(archived.embedding.len(), 3);
        // Note: rkyv strings are not standard rust strings, need conversion or direct comparison
        assert_eq!(archived.metadata, "{\"name\": \"Alice\"}");
    }

    #[test]
    fn test_rkyv_rejects_truncated_archive() {
        let node = Node {
            id: 1,
            embedding: vec![0.1, 0.2, 0.3],
            metadata: "{\"name\": \"Alice\"}".to_string(),
        };

        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&node).expect("failed to serialize");
        let truncated = &bytes[..bytes.len() / 2];

        let result = rkyv::access::<ArchivedNode, rkyv::rancor::Error>(truncated);
        assert!(
            result.is_err(),
            "truncated archive must be rejected by validation, not cause UB"
        );
    }
}
