use thiserror::Error;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{source}: {string:?}")]
pub(crate) struct TryFromStringError<E> {
    pub(crate) source: E,
    pub(crate) string: String,
}

macro_rules! validstr {
    ($t:ident, $err:ty, $validator:ident, $expecting:literal) => {
        impl From<$t> for String {
            fn from(value: $t) -> String {
                value.0.into()
            }
        }

        impl std::fmt::Debug for $t {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self.0)
            }
        }

        impl std::fmt::Display for $t {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl PartialEq<str> for $t {
            fn eq(&self, other: &str) -> bool {
                self.0 == other
            }
        }

        impl<'a> PartialEq<&'a str> for $t {
            fn eq(&self, other: &&'a str) -> bool {
                &self.0 == other
            }
        }

        impl AsRef<str> for $t {
            fn as_ref(&self) -> &str {
                self.0.as_ref()
            }
        }

        impl std::ops::Deref for $t {
            type Target = str;

            fn deref(&self) -> &str {
                &*self.0
            }
        }

        impl std::str::FromStr for $t {
            type Err = $err;

            fn from_str(s: &str) -> Result<$t, $err> {
                match $validator(s) {
                    Ok(()) => Ok($t(s.into())),
                    Err(e) => Err(e),
                }
            }
        }

        impl TryFrom<String> for $t {
            type Error = crate::validstr::TryFromStringError<$err>;

            fn try_from(s: String) -> Result<$t, Self::Error> {
                match $validator(&s) {
                    Ok(()) => Ok($t(s.into())),
                    Err(source) => Err(crate::validstr::TryFromStringError { source, string: s }),
                }
            }
        }

        #[allow(unused_qualifications)]
        impl serde::Serialize for $t {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::ser::Serializer,
            {
                serializer.serialize_str(self.as_ref())
            }
        }

        #[allow(unused_qualifications)]
        impl<'de> serde::Deserialize<'de> for $t {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::de::Deserializer<'de>,
            {
                struct Visitor;

                impl serde::de::Visitor<'_> for Visitor {
                    type Value = $t;

                    fn expecting(
                        &self,
                        formatter: &mut std::fmt::Formatter<'_>,
                    ) -> std::fmt::Result {
                        formatter.write_str($expecting)
                    }

                    fn visit_str<E>(self, input: &str) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        input
                            .parse::<$t>()
                            .map_err(|_| E::invalid_value(serde::de::Unexpected::Str(input), &self))
                    }

                    fn visit_string<E>(self, input: String) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        if $validator(&input).is_ok() {
                            Ok($t(input.into()))
                        } else {
                            Err(E::invalid_value(serde::de::Unexpected::Str(&input), &self))
                        }
                    }
                }

                deserializer.deserialize_string(Visitor)
            }
        }
    };
}
