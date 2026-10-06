import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { emptyDraft } from "./draft";
import { KsqlTab } from "./KsqlTab";

describe("KsqlTab", () => {
  it("shows the endpoint and its credential", () => {
    render(<KsqlTab draft={emptyDraft()} onChange={vi.fn()} />);

    expect(screen.getByLabelText("Endpoint")).toBeInTheDocument();
    expect(screen.getByLabelText("Basic auth credentials")).toBeInTheDocument();
  });

  it("reports what the user types into the endpoint", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<KsqlTab draft={emptyDraft()} onChange={onChange} />);

    await user.type(screen.getByLabelText("Endpoint"), "h");

    expect(onChange).toHaveBeenCalledWith({ ksqldbEndpoint: "h" });
  });

  it("reports what the user types into the credential", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<KsqlTab draft={emptyDraft()} onChange={onChange} />);

    await user.type(screen.getByLabelText("Basic auth credentials"), "u");

    expect(onChange).toHaveBeenCalledWith({ ksqldbBasicAuthCredentials: "u" });
  });

  // A credential on screen in plain text is a credential read over someone's
  // shoulder, the same reasoning every other secret field here follows.
  it("masks the credential", () => {
    render(<KsqlTab draft={emptyDraft()} onChange={vi.fn()} />);

    expect(screen.getByLabelText("Basic auth credentials")).toHaveAttribute("type", "password");
  });

  // Most clusters run no ksqlDB server; leaving this blank must not read as an
  // unfinished connection.
  it("says the server is optional", () => {
    render(<KsqlTab draft={emptyDraft()} onChange={vi.fn()} />);

    expect(screen.getByText(/Optional/)).toBeInTheDocument();
  });

  it("disables every field when the connection is connected", () => {
    render(<KsqlTab draft={emptyDraft()} onChange={vi.fn()} disabled />);

    expect(screen.getByLabelText("Endpoint")).toBeDisabled();
    expect(screen.getByLabelText("Basic auth credentials")).toBeDisabled();
  });
});
