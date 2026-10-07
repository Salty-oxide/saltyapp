import { useState } from "react";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { HeaderFilterPanel } from "./HeaderFilterPanel";
import { emptyHeaderRow, HeaderFilterRow } from "./headerFilters";

function Harness({
  initial = [emptyHeaderRow()],
  keys = ["env", "source"],
  appliedCount = 0,
  onApply = vi.fn(),
  onClear = vi.fn(),
}: {
  initial?: HeaderFilterRow[];
  keys?: string[];
  appliedCount?: number;
  onApply?: () => void;
  onClear?: () => void;
}) {
  const [rows, setRows] = useState(initial);
  return (
    <HeaderFilterPanel
      rows={rows}
      availableKeys={keys}
      appliedCount={appliedCount}
      onChange={setRows}
      onApply={onApply}
      onClear={onClear}
    />
  );
}

describe("HeaderFilterPanel", () => {
  it("starts with one key/value row, and Filter disabled until it is complete", async () => {
    const user = userEvent.setup();
    render(<Harness />);

    expect(screen.getAllByLabelText(/^Header value/)).toHaveLength(1);
    expect(screen.getByRole("button", { name: "Filter" })).toBeDisabled();

    await user.click(screen.getByRole("button", { name: /Select key/ }));
    await user.click(screen.getByRole("option", { name: "source" }));
    expect(screen.getByRole("button", { name: "Filter" })).toBeDisabled();

    await user.type(screen.getByLabelText("Header value 1"), "billing");
    expect(screen.getByRole("button", { name: "Filter" })).toBeEnabled();
  });

  it("keeps Filter disabled for a whitespace-only value", async () => {
    const user = userEvent.setup();
    render(<Harness initial={[{ ...emptyHeaderRow(), key: "source" }]} />);

    await user.type(screen.getByLabelText("Header value 1"), "   ");

    expect(screen.getByRole("button", { name: "Filter" })).toBeDisabled();
  });

  it("offers the topic's header keys in the dropdown", async () => {
    const user = userEvent.setup();
    render(<Harness keys={["env", "source"]} />);

    await user.click(screen.getByRole("button", { name: /Select key/ }));

    const options = within(screen.getByRole("listbox", { name: "Header key 1" })).getAllByRole("option");
    expect(options.map((o) => o.textContent)).toEqual(["✓ Select key", "env", "source"]);
  });

  it("still lists a row's own key when it is not among the loaded keys", async () => {
    const user = userEvent.setup();
    render(<Harness keys={[]} initial={[{ ...emptyHeaderRow(), key: "trace-id", value: "x" }]} />);

    await user.click(screen.getByRole("button", { name: /trace-id/ }));

    expect(screen.getByRole("option", { name: "✓ trace-id" })).toBeInTheDocument();
  });

  it("adds a row with Add header, and removes one with its Remove button", async () => {
    const user = userEvent.setup();
    render(<Harness />);

    await user.click(screen.getByRole("button", { name: "Add header" }));
    expect(screen.getAllByLabelText(/^Header value/)).toHaveLength(2);

    await user.click(screen.getByRole("button", { name: "Remove header 2" }));
    expect(screen.getAllByLabelText(/^Header value/)).toHaveLength(1);
  });

  it("will not remove the last remaining row", () => {
    render(<Harness />);
    expect(screen.getByRole("button", { name: "Remove header 1" })).toBeDisabled();
  });

  it("calls onApply from the Filter button and from Enter in a value input", async () => {
    const user = userEvent.setup();
    const onApply = vi.fn();
    render(<Harness initial={[{ ...emptyHeaderRow(), key: "source", value: "billing" }]} onApply={onApply} />);

    await user.click(screen.getByRole("button", { name: "Filter" }));
    expect(onApply).toHaveBeenCalledTimes(1);

    await user.type(screen.getByLabelText("Header value 1"), "{Enter}");
    expect(onApply).toHaveBeenCalledTimes(2);
  });

  it("does not apply on Enter while the rows are incomplete", async () => {
    const user = userEvent.setup();
    const onApply = vi.fn();
    render(<Harness onApply={onApply} />);

    await user.type(screen.getByLabelText("Header value 1"), "x{Enter}");

    expect(onApply).not.toHaveBeenCalled();
  });

  it("enables Clear when a filter is applied or a row has input, and calls onClear", async () => {
    const user = userEvent.setup();
    const onClear = vi.fn();
    const { unmount } = render(<Harness onClear={onClear} />);
    expect(screen.getByRole("button", { name: "Clear" })).toBeDisabled();
    unmount();

    render(<Harness appliedCount={1} onClear={onClear} />);
    await user.click(screen.getByRole("button", { name: "Clear" }));
    expect(onClear).toHaveBeenCalledTimes(1);
  });
});
