use serde::de::{DeserializeOwned, Error};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{from_value, Value};

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum VariadicValue<T> {
    /// None
    Null,
    /// Single
    Single(T),
    /// List
    Multiple(Vec<T>),
}

impl<T> From<VariadicValue<T>> for Option<Vec<T>> {
    fn from(v: VariadicValue<T>) -> Self {
        match v {
            VariadicValue::Null => None,
            VariadicValue::Single(x) => Some(vec![x]),
            VariadicValue::Multiple(xs) => Some(xs),
        }
    }
}

impl<T> VariadicValue<T> {
    pub fn iter<'a>(&'a self) -> Box<dyn std::iter::Iterator<Item = &T> + 'a> {
        match self {
            VariadicValue::Null => Box::new(std::iter::empty()),
            VariadicValue::Single(x) => Box::new(std::iter::once(x)),
            VariadicValue::Multiple(xs) => Box::new(xs.iter()),
        }
    }
}

impl<T> Serialize for VariadicValue<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self {
            VariadicValue::Null => serializer.serialize_none(),
            VariadicValue::Single(x) => x.serialize(serializer),
            VariadicValue::Multiple(xs) => xs.serialize(serializer),
        }
    }
}

impl<'a, T> Deserialize<'a> for VariadicValue<T>
where
    T: DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<VariadicValue<T>, D::Error>
    where
        D: Deserializer<'a>,
    {
        let v: Value = Deserialize::deserialize(deserializer)?;

        if v.is_null() {
            return Ok(VariadicValue::Null);
        }

        from_value(v.clone())
            .map(VariadicValue::Single)
            .or_else(|_| from_value(v).map(VariadicValue::Multiple))
            .map_err(|err| D::Error::custom(format!("Invalid variadic value type: {}", err)))
    }
}
