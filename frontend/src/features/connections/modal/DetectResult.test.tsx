import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import type { ClusterVersionReport } from "../../../lib/tauri";
import { DetectResult } from "./DetectResult";

const kraftReport: ClusterVersionReport = {
  mode: "kraft",
  processRoles: "broker,controller",
  interBrokerProtocolVersion: "4.1-IV0",
  suggestedVersion: "4.1",
  note: "On a KRaft cluster the authoritative metadata level is metadata.version.",
};

function idle() {
  return { isSuccess: false, isError: false, data: undefined, error: null } as const;
}

describe("DetectResult", () => {
  it("renders nothing before a detection has run", () => {
    const { container } = render(<DetectResult mutation={idle()} />);
    expect(container).toBeEmptyDOMElement();
  });

  it("reports the mode and the configs it was derived from", () => {
    render(
      <DetectResult mutation={{ isSuccess: true, isError: false, data: kraftReport, error: null }} />,
    );

    expect(screen.getByText("KRaft mode")).toBeInTheDocument();
    expect(screen.getByText(/process\.roles = broker,controller/)).toBeInTheDocument();
    expect(screen.getByText(/inter\.broker\.protocol\.version = 4\.1-IV0/)).toBeInTheDocument();
    expect(screen.getByText(/metadata\.version/)).toBeInTheDocument();
  });

  it("shows an empty process.roles as empty rather than as a blank line", () => {
    const zookeeper: ClusterVersionReport = {
      mode: "zookeeper",
      processRoles: "",
      interBrokerProtocolVersion: "3.9-IV0",
      suggestedVersion: "3.9",
      note: null,
    };
    render(<DetectResult mutation={{ isSuccess: true, isError: false, data: zookeeper, error: null }} />);

    expect(screen.getByText("ZooKeeper mode")).toBeInTheDocument();
    expect(screen.getByText(/process\.roles = \(empty\)/)).toBeInTheDocument();
  });

  it("explains an undeterminable mode instead of claiming one", () => {
    const unknown: ClusterVersionReport = {
      mode: "unknown",
      processRoles: null,
      interBrokerProtocolVersion: null,
      suggestedVersion: null,
      note: "The broker returned neither config.",
    };
    render(<DetectResult mutation={{ isSuccess: true, isError: false, data: unknown, error: null }} />);

    expect(screen.getByText("Mode could not be determined")).toBeInTheDocument();
    expect(screen.getByText("The broker returned neither config.")).toBeInTheDocument();
    expect(screen.queryByText(/process\.roles/)).not.toBeInTheDocument();
  });

  it("surfaces the backend's own message when detection fails", () => {
    render(
      <DetectResult
        mutation={{
          isSuccess: false,
          isError: true,
          data: undefined,
          error: new Error("failed to reach the cluster"),
        }}
      />,
    );

    expect(screen.getByRole("alert")).toHaveTextContent("failed to reach the cluster");
  });

  it("says so when applying the detected version is what hid the zookeeper section", () => {
    render(
      <DetectResult
        mutation={{ isSuccess: true, isError: false, data: kraftReport, error: null }}
        hidZookeeper
      />,
    );

    expect(screen.getByText(/ZooKeeper settings are hidden/)).toBeInTheDocument();
  });

  it("stays quiet about zookeeper when nothing was hidden", () => {
    render(
      <DetectResult mutation={{ isSuccess: true, isError: false, data: kraftReport, error: null }} />,
    );

    expect(screen.queryByText(/ZooKeeper settings are hidden/)).not.toBeInTheDocument();
  });
});
