use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[archive_attr(repr(C))]
pub struct Node {
    pub id: u64,
    pub embedding: Vec<f32>,
    pub metadata: String, // Simulating JSON for now
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[archive_attr(repr(C))]
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
        let bytes = rkyv::to_bytes::<_, 256>(&node).expect("failed to serialize");

        // Deserialize (Zero-copy access)
        let archived = unsafe { rkyv::archived_root::<Node>(&bytes[..]) };

        assert_eq!(archived.id, 1);
        assert_eq!(archived.embedding.len(), 3);
        // Note: rkyv strings are not standard rust strings, need conversion or direct comparison
        assert_eq!(archived.metadata, "{\"name\": \"Alice\"}");
    }
}
