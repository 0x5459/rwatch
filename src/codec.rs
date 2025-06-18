use deadpool_redis::redis::ToRedisArgs;
use serde::Deserialize;
use serde::Serialize;

pub trait Encode {
    type Error;

    fn encode(value: &impl Serialize) -> Result<impl ToRedisArgs, Self::Error>;
}

pub trait Decode {
    type Error;

    fn decode<T: for<'de> Deserialize<'de>>(value: &[u8]) -> Result<T, Self::Error>;
}

#[derive(Debug, Clone, Default)]
pub struct JsonEncode;

impl Encode for JsonEncode {
    type Error = serde_json::Error;

    fn encode(value: &impl Serialize) -> Result<impl ToRedisArgs, Self::Error> {
        serde_json::to_string(value)
    }
}

#[derive(Debug, Clone, Default)]
pub struct JsonDecode;

impl Decode for JsonDecode {
    type Error = serde_json::Error;
    fn decode<T>(v: &[u8]) -> Result<T, Self::Error>
    where T: for<'de> Deserialize<'de> {
        serde_json::from_slice(v)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde::Serialize;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestStruct {
        name: String,
        age: u32,
        active: bool,
    }

    #[test]
    fn test_json_decode_roundtrip() {
        let test_data = TestStruct {
            name: "Bob".to_string(),
            age: 25,
            active: false,
        };

        // Encode and decode roundtrip
        let json_str = serde_json::to_string(&test_data).expect("Failed to serialize");
        let decoded: TestStruct =
            JsonDecode::decode(json_str.as_bytes()).expect("Failed to decode");

        assert_eq!(decoded, test_data);
    }

    #[test]
    fn test_json_decode_error_cases() {
        // Invalid JSON
        let result: Result<TestStruct, _> = JsonDecode::decode(b"{ invalid json }");
        assert!(result.is_err());

        // Missing required fields
        let result: Result<TestStruct, _> = JsonDecode::decode(b"{\"name\": \"test\"}");
        assert!(result.is_err());
    }

    #[test]
    fn test_primitive_and_collection_types() {
        // Test number
        let number = 42i32;
        let json_str = serde_json::to_string(&number).unwrap();
        let decoded: i32 = JsonDecode::decode(json_str.as_bytes()).unwrap();
        assert_eq!(decoded, number);

        // Test string
        let text = "Hello, World!";
        let json_str = serde_json::to_string(&text).unwrap();
        let decoded: String = JsonDecode::decode(json_str.as_bytes()).unwrap();
        assert_eq!(decoded, text);

        // Test vector
        let vec_data = vec![1, 2, 3, 4, 5];
        let json_str = serde_json::to_string(&vec_data).unwrap();
        let decoded: Vec<i32> = JsonDecode::decode(json_str.as_bytes()).unwrap();
        assert_eq!(decoded, vec_data);
    }
}
