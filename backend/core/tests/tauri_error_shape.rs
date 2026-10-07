//! Type-checks `src-tauri`'s error plumbing against the real `error-stack`.
//!
//! `src-tauri` needs GTK/pkg-config and cannot be compiled in every
//! environment this workspace is developed in, so the parts of its command
//! layer that the error-stack 0.5 -> 0.8 upgrade touched — `CommandError`'s
//! `From<Report<AppError>>`, and the two helpers that walk a report's frames
//! — are mirrored here verbatim. `AttachmentKind::Printable` is the piece
//! worth pinning: 0.8 renamed `attach_printable` to `attach` and the old
//! opaque `attach` to `attach_opaque`, so a frame walker that still matched
//! the old variant would compile in 0.5 and silently match nothing in 0.8.

use error_stack::{Report, ResultExt};
use salty_core::AppError;

#[derive(serde::Serialize)]
pub struct CommandError {
    pub message: String,
}

impl From<Report<AppError>> for CommandError {
    fn from(report: Report<AppError>) -> Self {
        CommandError {
            message: format_report(&report),
        }
    }
}

fn format_report(report: &Report<AppError>) -> String {
    let mut parts = vec![report.current_context().to_string()];
    parts.push(report_reasons(report));
    parts.retain(|part| !part.is_empty());
    parts.join(": ")
}

fn report_reasons(report: &Report<AppError>) -> String {
    use error_stack::{AttachmentKind, FrameKind};

    report
        .frames()
        .filter_map(|frame| match frame.kind() {
            FrameKind::Attachment(AttachmentKind::Printable(printable)) => {
                Some(printable.to_string())
            }
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(": ")
}

fn failing() -> Result<(), Report<AppError>> {
    Err(Report::new(AppError::Validation))
        .attach("the outer reason")
        .attach("the inner reason")
}

#[test]
fn renders_every_attached_reason_into_one_line() {
    let error = failing().unwrap_err();

    let rendered = format_report(&error);

    // The context heading first, then each attachment — which is what the
    // frontend shows verbatim.
    assert!(
        rendered.starts_with(&AppError::Validation.to_string()),
        "got {rendered}"
    );
    assert!(rendered.contains("the outer reason"), "got {rendered}");
    assert!(rendered.contains("the inner reason"), "got {rendered}");
}

/// The frame walk is the part an API rename breaks silently: it would still
/// compile and simply stop finding anything.
#[test]
fn the_frame_walk_still_finds_attachments() {
    let error = failing().unwrap_err();

    assert!(
        !report_reasons(&error).is_empty(),
        "no printable attachments were found"
    );
}

#[test]
fn a_report_with_no_attachments_renders_just_its_context() {
    let error: Report<AppError> = Report::new(AppError::Decode);

    assert_eq!(format_report(&error), AppError::Decode.to_string());
}

/// Tauri's IPC is JSON, so whatever the commands return has to serialise.
#[test]
fn command_error_serialises_for_the_ipc_boundary() {
    let error: CommandError = failing().unwrap_err().into();

    let json = serde_json::to_value(&error).unwrap();
    assert!(
        json["message"]
            .as_str()
            .unwrap()
            .contains("the inner reason")
    );
}
