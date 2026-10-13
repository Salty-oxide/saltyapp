import { beforeEach, describe, expect, it, vi } from "vitest";
import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { CloseConfirmation } from "./CloseConfirmation";
import { closeAppWindow, onCloseRequested } from "../../lib/appWindow";

let requestClose: (() => void) | null = null;
const unlisten = vi.fn();
vi.mock("../../lib/appWindow", () => ({
  closeAppWindow: vi.fn().mockResolvedValue(undefined),
  onCloseRequested: vi.fn((handler: () => void) => {
    requestClose = handler;
    return Promise.resolve(unlisten);
  }),
}));

beforeEach(() => {
  requestClose = null;
  vi.mocked(closeAppWindow).mockClear();
  unlisten.mockClear();
});

async function renderAndRequestClose() {
  render(<CloseConfirmation />);
  await vi.waitFor(() => expect(requestClose).not.toBeNull());
  act(() => requestClose?.());
}

describe("CloseConfirmation", () => {
  it("shows nothing until the window asks to close", async () => {
    render(<CloseConfirmation />);
    await vi.waitFor(() => expect(onCloseRequested).toHaveBeenCalled());
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("asks for confirmation when the window is asked to close, without closing", async () => {
    await renderAndRequestClose();

    expect(screen.getByRole("dialog", { name: "Close application" })).toBeInTheDocument();
    expect(closeAppWindow).not.toHaveBeenCalled();
  });

  it("closes the app when confirmed", async () => {
    const user = userEvent.setup();
    await renderAndRequestClose();

    await user.click(screen.getByRole("button", { name: "Close application" }));

    expect(closeAppWindow).toHaveBeenCalledTimes(1);
  });

  it("stays open when cancelled, and asks again on the next close", async () => {
    const user = userEvent.setup();
    await renderAndRequestClose();

    await user.click(screen.getByRole("button", { name: "Cancel" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(closeAppWindow).not.toHaveBeenCalled();

    act(() => requestClose?.());
    expect(screen.getByRole("dialog", { name: "Close application" })).toBeInTheDocument();
  });

  it("cancels on Escape", async () => {
    const user = userEvent.setup();
    await renderAndRequestClose();

    await user.keyboard("{Escape}");

    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(closeAppWindow).not.toHaveBeenCalled();
  });

  it("stops listening when unmounted", async () => {
    const { unmount } = render(<CloseConfirmation />);
    await vi.waitFor(() => expect(requestClose).not.toBeNull());
    unmount();
    await vi.waitFor(() => expect(unlisten).toHaveBeenCalled());
  });
});
