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
}: {
  initial?: HeaderFilterRow[];
  keys?: string[];
  appliedCount?: number;
  onApply?: (rows: HeaderFilterRow[]) => void;
}) {
  const [rows, setRows] = useState(initial);
  return (
    <HeaderFilterPanel
      rows={rows}
      availableKeys={keys}
      appliedCount={appliedCount}
      onChange={setRows}
      onApply={onApply}
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

    const options = within(
      screen.getByRole("listbox", { name: "Header key 1" }),
    ).getAllByRole("option");
    expect(options.map((o) => o.textContent)).toEqual([
      "✓ Select key",
      "env",
      "source",
    ]);
  });

  it("still lists a row's own key when it is not among the loaded keys", async () => {
    const user = userEvent.setup();
    render(
      <Harness
        keys={[]}
        initial={[{ ...emptyHeaderRow(), key: "trace-id", value: "x" }]}
      />,
    );

    await user.click(screen.getByRole("button", { name: /trace-id/ }));

    expect(
      screen.getByRole("option", { name: "✓ trace-id" }),
    ).toBeInTheDocument();
  });

  it("adds a row below with the + button, and removes it with its Delete button", async () => {
    const user = userEvent.setup();
    render(<Harness />);

    await user.click(screen.getByRole("button", { name: "Add header" }));
    expect(screen.getAllByLabelText(/^Header value/)).toHaveLength(2);

    await user.click(screen.getByRole("button", { name: "Delete header 2" }));
    expect(screen.getAllByLabelText(/^Header value/)).toHaveLength(1);
  });

  it("offers Delete only from the second header on", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    expect(
      screen.queryByRole("button", { name: /^Delete header/ }),
    ).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Add header" }));
    expect(
      screen.queryByRole("button", { name: "Delete header 1" }),
    ).not.toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Delete header 2" }),
    ).toBeInTheDocument();
  });

  /**
   * Layout contract, asserted on the icon cluster's children because the gap
   * is an empty placeholder element, which has no role to query by.
   */
  describe("icon order", () => {
    function iconsOfRow(n: number) {
      const row = screen.getByLabelText(`Header value ${n}`).closest(".header-filter-row")!;
      return Array.from(row.querySelector(".header-filter-icons")!.children);
    }
    const labelOf = (el: Element) => el.getAttribute("aria-label");

    it("sits Add directly beside Clear on the only row, with no gap between them", () => {
      render(<Harness />);

      const icons = iconsOfRow(1);

      expect(icons.map(labelOf)).toEqual(["Clear header 1", "Add header"]);
    });

    it("puts Delete in the middle and Add on the right of the last row once there are two", async () => {
      const user = userEvent.setup();
      render(<Harness />);

      await user.click(screen.getByRole("button", { name: "Add header" }));

      expect(iconsOfRow(2).map(labelOf)).toEqual(["Clear header 2", "Delete header 2", "Add header"]);
    });

    it("keeps the first row's Clear in the same column as the last row's, with the other two columns held open", async () => {
      const user = userEvent.setup();
      render(<Harness />);

      await user.click(screen.getByRole("button", { name: "Add header" }));

      const first = iconsOfRow(1);
      expect(first).toHaveLength(3);
      expect(labelOf(first[0])).toBe("Clear header 1");
      expect(first[1]).toHaveClass("header-filter-icon-slot");
      expect(first[2]).toHaveClass("header-filter-icon-slot");
    });

    it("gives a middle row Clear and Delete, and holds open the Add column", async () => {
      const user = userEvent.setup();
      render(<Harness />);
      await user.click(screen.getByRole("button", { name: "Add header" }));
      await user.click(screen.getByRole("button", { name: "Add header" }));

      const middle = iconsOfRow(2);

      expect(middle).toHaveLength(3);
      expect(labelOf(middle[0])).toBe("Clear header 2");
      expect(labelOf(middle[1])).toBe("Delete header 2");
      expect(middle[2]).toHaveClass("header-filter-icon-slot");
    });

    it("closes the gap again when rows are deleted back down to one", async () => {
      const user = userEvent.setup();
      render(<Harness />);
      await user.click(screen.getByRole("button", { name: "Add header" }));

      await user.click(screen.getByRole("button", { name: "Delete header 2" }));

      expect(iconsOfRow(1).map(labelOf)).toEqual(["Clear header 1", "Add header"]);
    });
  });

  it("x clears that row's key and value without removing the row", async () => {
    const user = userEvent.setup();
    render(
      <Harness
        initial={[{ ...emptyHeaderRow(), key: "source", value: "billing" }]}
      />,
    );

    await user.click(screen.getByRole("button", { name: "Clear header 1" }));

    expect(screen.getByLabelText("Header value 1")).toHaveValue("");
    expect(
      screen.getByRole("button", { name: /Select key/ }),
    ).toBeInTheDocument();
  });

  it("x also applies the emptied rows, so the grid filter is removed", async () => {
    const user = userEvent.setup();
    const onApply = vi.fn();
    render(
      <Harness
        initial={[{ ...emptyHeaderRow(), key: "source", value: "billing" }]}
        appliedCount={1}
        onApply={onApply}
      />,
    );

    await user.click(screen.getByRole("button", { name: "Clear header 1" }));

    expect(onApply).toHaveBeenCalledTimes(1);
    expect(onApply.mock.calls[0][0]).toEqual([
      expect.objectContaining({ key: "", value: "" }),
    ]);
  });

  it("has no Clear or text Add header button, and shows no field captions", () => {
    render(<Harness />);
    expect(
      screen.queryByRole("button", { name: "Clear" }),
    ).not.toBeInTheDocument();
    expect(screen.queryByText("Add header")).not.toBeInTheDocument();
    expect(screen.getByPlaceholderText("Value")).toBeInTheDocument();
  });

  it("calls onApply from the Filter button and from Enter in a value input", async () => {
    const user = userEvent.setup();
    const onApply = vi.fn();
    render(
      <Harness
        initial={[{ ...emptyHeaderRow(), key: "source", value: "billing" }]}
        onApply={onApply}
      />,
    );

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

  it("keeps Filter enabled while a filter is applied, so clearing rows can drop it", async () => {
    const user = userEvent.setup();
    const onApply = vi.fn();
    render(<Harness appliedCount={1} onApply={onApply} />);

    await user.click(screen.getByRole("button", { name: "Filter" }));
    expect(onApply).toHaveBeenCalledTimes(1);
  });
});
