use crate::{ActorRef, OperationId};

const HANDLER_MAX: usize = 128;
const EXCEPTION_MAX: usize = 128;
const FILE_MAX: usize = 256;
const FUNCTION_MAX: usize = 128;
const MAX_FRAMES: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailurePhase {
    Construction,
    Configure,
    Start,
    Handler,
    Stop,
    Supervisor,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailureFrame {
    file: Box<str>,
    line: u32,
    function: Box<str>,
    truncated: bool,
}

impl FailureFrame {
    pub fn new(file: &str, line: u32, function: &str) -> Self {
        Self {
            file: bounded(file, FILE_MAX),
            line,
            function: bounded(function, FUNCTION_MAX),
            truncated: file.len() > FILE_MAX || function.len() > FUNCTION_MAX,
        }
    }

    pub fn file(&self) -> &str {
        &self.file
    }
    pub fn line(&self) -> u32 {
        self.line
    }
    pub fn function(&self) -> &str {
        &self.function
    }
    pub fn truncated(&self) -> bool {
        self.truncated
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailureDetails {
    phase: FailurePhase,
    handler: Box<str>,
    exception_type: Box<str>,
    frames: Box<[FailureFrame]>,
    truncated: bool,
}

impl FailureDetails {
    pub fn new(
        phase: FailurePhase,
        handler: &str,
        exception_type: &str,
        frames: Vec<FailureFrame>,
    ) -> Self {
        let frames_truncated = frames.len() > MAX_FRAMES;
        let text_truncated = handler.len() > HANDLER_MAX || exception_type.len() > EXCEPTION_MAX;
        let frames = frames
            .into_iter()
            .take(MAX_FRAMES)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let frame_text_truncated = frames.iter().any(FailureFrame::truncated);
        Self {
            phase,
            handler: bounded(handler, HANDLER_MAX),
            exception_type: bounded(exception_type, EXCEPTION_MAX),
            frames,
            truncated: frames_truncated || frame_text_truncated || text_truncated,
        }
    }

    /// Marks externally observed truncation in addition to truncation performed
    /// by this constructor. This is used when an upstream boundary has already
    /// discarded information before constructing the record.
    pub fn with_truncated(mut self, truncated: bool) -> Self {
        self.truncated |= truncated;
        self
    }

    pub fn phase(&self) -> FailurePhase {
        self.phase
    }
    pub fn handler(&self) -> &str {
        &self.handler
    }
    pub fn exception_type(&self) -> &str {
        &self.exception_type
    }
    pub fn frames(&self) -> &[FailureFrame] {
        &self.frames
    }
    pub fn truncated(&self) -> bool {
        self.truncated
    }
}

fn bounded(value: &str, max: usize) -> Box<str> {
    let mut end = value.len().min(max);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned().into_boxed_str()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureAction {
    StopActor,
    StopWorld,
    Continue,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailureRecord {
    pub sequence: u64,
    pub elapsed_ns: u64,
    pub actor: ActorRef,
    pub event_id: Option<u64>,
    pub schema: Option<u32>,
    pub operation: Option<OperationId>,
    pub details: FailureDetails,
    pub action: FailureAction,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_are_utf8_safe() {
        let details = FailureDetails::new(
            FailurePhase::Handler,
            &"é".repeat(200),
            &"λ".repeat(200),
            vec![FailureFrame::new(&"é".repeat(300), 7, &"λ".repeat(200))],
        );
        assert!(details.handler().len() <= HANDLER_MAX);
        assert!(details.exception_type().len() <= EXCEPTION_MAX);
        assert!(details.frames()[0].file().len() <= FILE_MAX);
        assert!(details.frames()[0].function().len() <= FUNCTION_MAX);
        assert!(details.handler().is_char_boundary(details.handler().len()));
        assert!(
            details.frames()[0]
                .file()
                .is_char_boundary(details.frames()[0].file().len())
        );
        assert!(details.frames()[0].truncated());
        assert!(details.truncated());
    }

    #[test]
    fn frame_limit_and_boxed_storage() {
        let frames = (0..20)
            .map(|i| FailureFrame::new("f", i, "h"))
            .collect::<Vec<_>>();
        let details = FailureDetails::new(FailurePhase::Start, "h", "E", frames);
        assert_eq!(details.frames().len(), MAX_FRAMES);
        assert!(details.truncated());
    }

    #[test]
    fn external_truncation_is_preserved() {
        let details = FailureDetails::new(FailurePhase::Stop, "handler", "Error", Vec::new());
        assert!(details.with_truncated(true).truncated());
    }

    #[test]
    fn source_capacity_is_not_retained() {
        let mut handler = String::with_capacity(1 << 20);
        handler.push_str("short");
        let details = FailureDetails::new(FailurePhase::Configure, &handler, "E", Vec::new());
        assert_eq!(details.handler(), "short");
        assert_eq!(details.handler().len(), 5);
    }
}
