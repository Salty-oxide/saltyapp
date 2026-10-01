import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { openUrl } from "@tauri-apps/plugin-opener";
import { KRaftNotice, KRAFT_DOCS_URL } from "./KRaftNotice";

vi.mock("@tauri-apps/plugin-opener", () => ({ openUrl: vi.fn() }));

beforeEach(() => {
  vi.clearAllMocks();
});

describe("KRaftNotice", () => {
  it("explains that Kafka 4 removed ZooKeeper and that clients do not contact controllers", () => {
    render(<KRaftNotice />);

    expect(screen.getByRole("heading", { name: "KRaft" })).toBeInTheDocument();
    expect(screen.getByText(/Kafka 4\.0 removed ZooKeeper/)).toBeInTheDocument();
    expect(screen.getByText(/never connect to/)).toBeInTheDocument();
  });

  it("opens the Kafka KRaft documentation", async () => {
    const user = userEvent.setup();
    render(<KRaftNotice />);

    await user.click(screen.getByRole("button", { name: "Learn more about KRaft" }));

    expect(openUrl).toHaveBeenCalledWith(KRAFT_DOCS_URL);
  });

  it("links only to the host the opener capability allows", () => {
    // src-tauri/capabilities/default.json scopes opener:allow-open-url to
    // https://kafka.apache.org/* — a URL outside it is rejected at runtime,
    // silently, so this is pinned here rather than discovered by a user.
    expect(KRAFT_DOCS_URL.startsWith("https://kafka.apache.org/")).toBe(true);
  });
});
