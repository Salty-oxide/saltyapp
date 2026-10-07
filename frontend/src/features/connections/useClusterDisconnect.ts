import { useEffect, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { ConnectionStatus } from "../../lib/tauri";
import { useLogsStore } from "../bottom-panel/useLogsStore";
import { clearConnectionState } from "./clearConnectionState";
import { useDisconnect } from "./useConnections";

/**
 * How many consecutive unreachable polls end a connected cluster's session.
 *
 * Two, for the same reason the auth breaker allows two attempts: one failed
 * TCP check is a blip — a laptop switching networks, a VPN reconnecting, a
 * broker mid-rolling-restart — and acting on it would throw away the user's
 * expanded tree, their fetched messages and their open payload for something
 * that fixes itself before they notice. Two consecutive failures — the second
 * confirmed `UNREACHABLE_RECHECK_MS` after the first, not a full steady
 * interval later — is a cluster that has actually gone.
 */
export const UNREACHABLE_POLLS_BEFORE_DISCONNECT = 2;

/**
 * Clears everything the app holds about a cluster the moment its session
 * ends, whatever ended it.
 *
 * Hung off the backend's own "is this connected" answer rather than off each
 * of the things that can disconnect it, because there are four and only one
 * of them is a button: Disconnect, the 120-minute idle timer, the auth
 * circuit breaker tripping inside the backend, and `useUnreachableDisconnect`
 * below. Watching the result instead of the causes means a route added later
 * is covered without knowing about this, and no route can clear half of it.
 */
export function useClusterDisconnectCleanup(connectionId: string, isConnected: boolean): void {
  const queryClient = useQueryClient();
  // Tracked with the id it was observed for: this hook runs inside a row of a
  // list, and a row re-keyed onto a different connection must not read the
  // previous one's connectedness as this one's history.
  const previous = useRef({ connectionId, isConnected });

  useEffect(() => {
    const wasConnected = previous.current.connectionId === connectionId && previous.current.isConnected;
    previous.current = { connectionId, isConnected };
    if (wasConnected && !isConnected) {
      clearConnectionState(queryClient, connectionId);
    }
  }, [connectionId, isConnected, queryClient]);
}

/**
 * Ends the session of a connected cluster that has stopped answering.
 *
 * Without this, losing the network left the app in its most misleading state:
 * the backend still held the connection in its connected set, so the tree
 * stayed expanded and every pane stayed populated with a cluster's contents,
 * while the only sign anything was wrong was the status dot turning red.
 * Disconnecting for real routes it through the same path as the button — the
 * backend drops its session and pooled clients, and
 * `useClusterDisconnectCleanup` clears the UI off the resulting transition.
 *
 * Counts polls rather than watching for a value change: the status query
 * keeps reporting `UNREACHABLE` with the same value each time, so the strikes
 * are counted off `statusUpdatedAt`, which moves on every completed poll.
 */
export function useUnreachableDisconnect(
  connectionId: string,
  isConnected: boolean,
  status: ConnectionStatus,
  statusUpdatedAt: number,
  connectionName: string,
): void {
  const disconnect = useDisconnect();
  const strikes = useRef(0);
  // Read through a ref so the effect can depend on the poll alone. Depending
  // on the mutation object would re-run it on every render of this row, and
  // re-running it is what counts a strike.
  const disconnectRef = useRef(disconnect);
  disconnectRef.current = disconnect;

  useEffect(() => {
    if (!isConnected || status !== "UNREACHABLE") {
      strikes.current = 0;
      return;
    }
    strikes.current += 1;
    if (strikes.current < UNREACHABLE_POLLS_BEFORE_DISCONNECT) return;
    strikes.current = 0;
    // Said out loud, because everything the user had open is about to
    // disappear and the status dot alone does not explain why.
    useLogsStore.getState().addEntry({
      timestamp: new Date().toISOString(),
      level: "warn",
      message: `${connectionName} became unreachable — disconnected, and its loaded data cleared. Use Connect once the cluster is back.`,
    });
    disconnectRef.current.mutate(connectionId);
    // `status` is in the deps for correctness on a change back to reachable;
    // `statusUpdatedAt` is what makes a repeated identical result count.
  }, [connectionId, connectionName, isConnected, status, statusUpdatedAt]);
}
