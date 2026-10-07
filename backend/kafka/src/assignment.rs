use error_stack::ResultExt;
use salty_core::AppError;
use salty_core::Result;

/// Decodes a `GroupMemberInfo::assignment()` byte slice (Kafka's
/// `ConsumerProtocolAssignment` wire format) into the flat list of
/// (topic, partition) pairs this member owns:
///
/// ```text
/// Assignment => Version AssignedPartitions UserData
///   Version => int16
///   AssignedPartitions => Array<Topic>
///     Topic => TopicName Array<Partition>
///       TopicName => string (int16 length-prefixed UTF-8)
///       Partition => int32
///   UserData => bytes (ignored — nothing here parses past AssignedPartitions)
/// ```
pub fn decode_consumer_protocol_assignment(bytes: &[u8]) -> Result<Vec<(String, i32)>, AppError> {
    let mut cursor = Cursor { bytes, pos: 0 };
    let _version = cursor.read_i16()?;
    let topic_count = cursor.read_i32()?;
    if topic_count < 0 {
        return Ok(Vec::new());
    }

    let mut result = Vec::new();
    for _ in 0..topic_count {
        let topic = cursor.read_string()?;
        let partition_count = cursor.read_i32()?;
        if partition_count < 0 {
            continue;
        }
        for _ in 0..partition_count {
            result.push((topic.clone(), cursor.read_i32()?));
        }
    }
    Ok(result)
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn read_i16(&mut self) -> Result<i16, AppError> {
        let slice = self
            .bytes
            .get(self.pos..self.pos + 2)
            .ok_or_else(|| error_stack::Report::new(AppError::Kafka))
            .attach("truncated assignment: expected i16")?;
        self.pos += 2;
        Ok(i16::from_be_bytes([slice[0], slice[1]]))
    }

    fn read_i32(&mut self) -> Result<i32, AppError> {
        let slice = self
            .bytes
            .get(self.pos..self.pos + 4)
            .ok_or_else(|| error_stack::Report::new(AppError::Kafka))
            .attach("truncated assignment: expected i32")?;
        self.pos += 4;
        Ok(i32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]))
    }

    fn read_string(&mut self) -> Result<String, AppError> {
        let len = self.read_i16()?;
        if len < 0 {
            return Err(error_stack::Report::new(AppError::Kafka))
                .attach("truncated assignment: null topic name");
        }
        let len = len as usize;
        let slice = self
            .bytes
            .get(self.pos..self.pos + len)
            .ok_or_else(|| error_stack::Report::new(AppError::Kafka))
            .attach("truncated assignment: expected topic name bytes")?;
        self.pos += len;
        String::from_utf8(slice.to_vec())
            .change_context(AppError::Kafka)
            .attach("assignment topic name is not valid UTF-8")
    }
}

#[cfg(test)]
mod tests {
    use super::decode_consumer_protocol_assignment;

    fn encode_i16(v: i16) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }
    fn encode_i32(v: i32) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }
    fn encode_string(s: &str) -> Vec<u8> {
        let mut out = encode_i16(s.len() as i16);
        out.extend_from_slice(s.as_bytes());
        out
    }

    #[test]
    fn decodes_a_single_topic_single_partition_assignment() {
        let mut bytes = encode_i16(0); // version
        bytes.extend(encode_i32(1)); // 1 topic
        bytes.extend(encode_string("orders"));
        bytes.extend(encode_i32(1)); // 1 partition
        bytes.extend(encode_i32(0)); // partition 0

        let result = decode_consumer_protocol_assignment(&bytes).unwrap();
        assert_eq!(result, vec![("orders".to_string(), 0)]);
    }

    #[test]
    fn decodes_a_single_topic_with_multiple_partitions() {
        let mut bytes = encode_i16(0);
        bytes.extend(encode_i32(1));
        bytes.extend(encode_string("orders"));
        bytes.extend(encode_i32(3));
        bytes.extend(encode_i32(0));
        bytes.extend(encode_i32(1));
        bytes.extend(encode_i32(2));

        let result = decode_consumer_protocol_assignment(&bytes).unwrap();
        assert_eq!(
            result,
            vec![
                ("orders".to_string(), 0),
                ("orders".to_string(), 1),
                ("orders".to_string(), 2),
            ]
        );
    }

    #[test]
    fn decodes_multiple_topics() {
        let mut bytes = encode_i16(0);
        bytes.extend(encode_i32(2));
        bytes.extend(encode_string("orders"));
        bytes.extend(encode_i32(1));
        bytes.extend(encode_i32(0));
        bytes.extend(encode_string("payments"));
        bytes.extend(encode_i32(1));
        bytes.extend(encode_i32(5));

        let result = decode_consumer_protocol_assignment(&bytes).unwrap();
        assert_eq!(
            result,
            vec![("orders".to_string(), 0), ("payments".to_string(), 5)]
        );
    }

    #[test]
    fn decodes_zero_topics_to_an_empty_list() {
        let mut bytes = encode_i16(0);
        bytes.extend(encode_i32(0));

        let result = decode_consumer_protocol_assignment(&bytes).unwrap();
        assert_eq!(result, Vec::new());
    }

    #[test]
    fn errors_on_truncated_input() {
        let bytes = encode_i16(0); // version only, missing topic count
        let result = decode_consumer_protocol_assignment(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn errors_on_a_topic_name_length_that_overruns_the_buffer() {
        let mut bytes = encode_i16(0);
        bytes.extend(encode_i32(1));
        bytes.extend(encode_i16(50)); // claims a 50-byte name
        bytes.extend(b"short"); // but only 5 bytes follow

        let result = decode_consumer_protocol_assignment(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn ignores_trailing_user_data_bytes() {
        let mut bytes = encode_i16(0);
        bytes.extend(encode_i32(1));
        bytes.extend(encode_string("orders"));
        bytes.extend(encode_i32(1));
        bytes.extend(encode_i32(0));
        bytes.extend(encode_i32(4)); // user_data length prefix
        bytes.extend(b"xyz!"); // user_data bytes (never read)

        let result = decode_consumer_protocol_assignment(&bytes).unwrap();
        assert_eq!(result, vec![("orders".to_string(), 0)]);
    }
}
