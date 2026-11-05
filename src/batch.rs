//! Request batches: several requests sent as one message and processed in order.

use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::enums::RequestBatchExecutionType;
use crate::error::{Error, Result};
use crate::protocol::{RequestBatchMessage, RequestMessage, RequestResponse, decode_response};
use crate::requests::Request;

/// A batch of requests for [`Client::batch`](crate::Client::batch).
///
/// [`push`](Self::push) returns a handle that reads that request's typed result from the
/// [`BatchResponse`].
///
/// ```no_run
/// # async fn demo(client: obsr::Client) -> obsr::Result<()> {
/// use obsr::RequestBatch;
/// use obsr::requests::{GetSceneList, GetVersion};
///
/// let mut batch = RequestBatch::new();
/// let version = batch.push(GetVersion)?;
/// let scenes = batch.push(GetSceneList::default())?;
///
/// let response = client.batch(batch).await?;
/// println!("{}", response.get(&version)?.obs_version);
/// println!("{} scenes", response.get(&scenes)?.scenes.len());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default)]
pub struct RequestBatch {
    halt_on_failure: Option<bool>,
    execution_type: Option<RequestBatchExecutionType>,
    requests: Vec<RequestMessage>,
}

impl RequestBatch {
    /// Creates an empty batch. The server's defaults apply: keep going after a failure, and
    /// [`SerialRealtime`](RequestBatchExecutionType::SerialRealtime) execution.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets whether the server stops at the first failed request.
    #[must_use]
    pub fn halt_on_failure(mut self, halt: bool) -> Self {
        self.halt_on_failure = Some(halt);
        self
    }

    /// Sets how the server executes the batch.
    ///
    /// [`None`](RequestBatchExecutionType::None) restores the server's default, since sending it
    /// would make the server close the connection.
    #[must_use]
    pub fn execution_type(mut self, execution_type: RequestBatchExecutionType) -> Self {
        self.execution_type =
            (execution_type != RequestBatchExecutionType::None).then_some(execution_type);
        self
    }

    /// Adds a request and returns the handle for its result.
    pub fn push<R: Request>(&mut self, request: R) -> Result<BatchHandle<R::Response>> {
        let data = crate::client::request_data(&request)?;
        Ok(self.add(R::REQUEST_TYPE, data))
    }

    /// Adds a request by name, whose result is read back as a raw [`Value`].
    pub fn push_raw(
        &mut self,
        request_type: impl Into<String>,
        data: Option<Value>,
    ) -> BatchHandle<Value> {
        self.add(request_type.into(), data)
    }

    /// The number of requests in the batch.
    pub fn len(&self) -> usize {
        self.requests.len()
    }

    /// Returns `true` if the batch has no requests.
    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    fn add<T>(&mut self, request_type: impl Into<String>, data: Option<Value>) -> BatchHandle<T> {
        let request_type = request_type.into();
        let request_id = self.requests.len().to_string();
        self.requests.push(RequestMessage {
            request_type: request_type.clone(),
            request_id: request_id.clone(),
            request_data: data,
        });
        BatchHandle {
            request_type,
            request_id,
            response: PhantomData,
        }
    }

    pub(crate) fn into_message(self, request_id: String) -> RequestBatchMessage {
        RequestBatchMessage {
            request_id,
            halt_on_failure: self.halt_on_failure,
            execution_type: self.execution_type,
            requests: self.requests,
        }
    }
}

/// Identifies one request's result within a [`BatchResponse`], and knows its type.
///
/// A handle is only meaningful for the response to the batch that created it.
#[derive(Debug, Clone)]
pub struct BatchHandle<T> {
    request_type: String,
    request_id: String,
    response: PhantomData<fn() -> T>,
}

/// The results of a request batch.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchResponse {
    results: Vec<RequestResponse>,
}

impl BatchResponse {
    pub(crate) fn new(results: Vec<RequestResponse>) -> Self {
        Self { results }
    }

    /// Decodes the result of one request.
    ///
    /// Returns [`Error::Request`] if that request failed, and [`Error::NotExecuted`] if it did not
    /// run because the batch halted first.
    pub fn get<T: DeserializeOwned>(&self, handle: &BatchHandle<T>) -> Result<T> {
        let result = self
            .results
            .iter()
            .find(|result| result.request_id.as_deref() == Some(&handle.request_id))
            .ok_or_else(|| Error::NotExecuted {
                request_type: handle.request_type.clone(),
            })?;
        let data = result.clone().into_result()?;
        decode_response(data, &handle.request_type)
    }

    /// The raw results, in the order the server returned them.
    pub fn results(&self) -> &[RequestResponse] {
        &self.results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_its_length_and_gives_each_request_its_own_id() {
        let mut batch = RequestBatch::new();
        assert!(batch.is_empty());
        let first = batch.push_raw("A", None);
        let second = batch.push_raw("B", None);
        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
        assert_ne!(first.request_id, second.request_id);
    }
}
