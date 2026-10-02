import { useMemo, useState } from "react";
import { Dropdown } from "../../../components/Dropdown";
import { isKRaftOnly, KAFKA_VERSIONS } from "../../../lib/tauri";
import { useDetectClusterVersion, usePingBootstrapServers, usePingZookeeper } from "../useConnections";
import { ConnectionDraft, toNewConnection } from "./draft";
import { DetectResult } from "./DetectResult";
import { KRaftNotice } from "./KRaftNotice";
import { PingResult } from "./PingResult";

const KAFKA_VERSION_OPTIONS = KAFKA_VERSIONS.map((version) => ({ id: version, label: version }));

/**
 * The offered versions, plus the draft's own value when it isn't one of them.
 *
 * `Dropdown` labels its toggle with `options.find(...) ?? options[0]`, so a
 * connection saved with a version this build no longer offers — 2.9, which
 * Kafka never released, was offered until recently — would *display* 0.11
 * while still storing 2.9, and Update would stay disabled because the draft
 * never diverged from its snapshot. Appending keeps the display truthful;
 * the value simply can't be newly selected. It also means a version Detect
 * reads off a future broker shows up rather than being swallowed.
 */
function versionOptions(current: string) {
  if (KAFKA_VERSION_OPTIONS.some((option) => option.id === current)) return KAFKA_VERSION_OPTIONS;
  return [...KAFKA_VERSION_OPTIONS, { id: current, label: current }];
}

export interface ConnectionTabProps {
  draft: ConnectionDraft;
  onChange: (patch: Partial<ConnectionDraft>) => void;
  /** When true, every field except Cluster Name is disabled — used by the cluster detail panel while connected. */
  disabled?: boolean;
}

export function PropertiesTab({ draft, onChange, disabled = false }: ConnectionTabProps) {
  const pingBootstrap = usePingBootstrapServers();
  const pingZookeeper = usePingZookeeper();
  const detect = useDetectClusterVersion();
  const [hidZookeeper, setHidZookeeper] = useState(false);
  const kafkaVersionOptions = useMemo(() => versionOptions(draft.kafkaVersion), [draft.kafkaVersion]);

  return (
    <div role="tabpanel" aria-label="Properties" className="connection-modal-tab-panel">
      <section className="connection-modal-section">
        <h3>General</h3>
        <label>
          Cluster name
          <input value={draft.name} onChange={(e) => onChange({ name: e.target.value })} />
        </label>
        <fieldset disabled={disabled} className="connection-modal-fieldset">
          <label>
            Bootstrap servers
            <div className="connection-modal-input-row">
              <input
                value={draft.bootstrapServers}
                onChange={(e) => onChange({ bootstrapServers: e.target.value })}
                placeholder="localhost:9092"
              />
              <button
                type="button"
                aria-label="Ping bootstrap servers"
                disabled={pingBootstrap.isPending || draft.bootstrapServers.trim().length === 0}
                onClick={() => pingBootstrap.mutate(draft.bootstrapServers.trim())}
              >
                Ping
              </button>
            </div>
          </label>
          <PingResult mutation={pingBootstrap} failureMessage="Unable to reach bootstrap servers" />
          <div className="connection-modal-input-row connection-modal-detect-row">
            <Dropdown
              label="Kafka cluster version"
              ariaLabel="Kafka cluster version"
              options={kafkaVersionOptions}
              displayedId={draft.kafkaVersion}
              appliedId={draft.kafkaVersion}
              onCommit={(id) => onChange({ kafkaVersion: id })}
            />
            <button
              type="button"
              aria-label="Detect cluster version"
              disabled={detect.isPending || draft.bootstrapServers.trim().length === 0}
              onClick={() =>
                detect.mutate(toNewConnection(draft), {
                  onSuccess: (report) => {
                    // Recomputed on *every* successful detect, before the
                    // early return below. Otherwise a detect that yields no
                    // suggestion leaves the previous run's value standing,
                    // and the note goes on claiming "this broker reports
                    // KRaft" directly beneath a result line reading "Mode
                    // could not be determined".
                    //
                    // Keyed on the *applied* version rather than the report's
                    // mode, so it marks the moment the section actually went
                    // away — not a broker reporting KRaft to a connection
                    // already on 4.x, which had no section to lose.
                    setHidZookeeper(
                      report.suggestedVersion !== null &&
                        !isKRaftOnly(draft.kafkaVersion) &&
                        isKRaftOnly(report.suggestedVersion),
                    );

                    // Applied, not asserted: on a KRaft cluster this is
                    // derived from inter.broker.protocol.version rather than
                    // from the authoritative metadata.version, so the user
                    // can still change it. An unlisted value is fine —
                    // `versionOptions` appends it.
                    if (report.suggestedVersion === null) return;
                    onChange({ kafkaVersion: report.suggestedVersion });
                  },
                })
              }
            >
              Detect
            </button>
          </div>
          {/* `&& isKRaftOnly(...)` so the note survives only while the
              section is actually gone. `hidZookeeper` records that a Detect
              *crossed* into KRaft-only territory, which is what makes the
              note worth showing at all — but it is set in `onSuccess` and
              never recomputed, so on its own it would keep claiming the
              section is hidden after the user manually picks 3.9 again, with
              the ZooKeeper fields rendered directly beneath the claim. */}
          <DetectResult
            mutation={detect}
            hidZookeeper={hidZookeeper && isKRaftOnly(draft.kafkaVersion)}
          />
        </fieldset>
      </section>

      <section className="connection-modal-section">
        <h3>Publishing</h3>
        {/* Deliberately outside the `disabled` fieldsets around it. Those are
            disabled while the cluster is connected because changing a
            connection's *identity* mid-session would leave the live client
            describing settings it was not built from. This flag changes nothing
            about the client — it is read fresh by the publish command every
            time — and being able to grant or revoke publishing on a cluster you
            are currently looking at is the point of it. */}
        <label className="connection-modal-checkbox-label">
          <input
            type="checkbox"
            checked={draft.allowPublishing}
            onChange={(e) => onChange({ allowPublishing: e.target.checked })}
          />
          Allow publishing messages to this cluster
        </label>
        <p className="connection-modal-hint">
          Off by default. The broker&apos;s own permissions always apply — this cannot grant write
          access you do not have, but leaving it off prevents publishing to this cluster by mistake.
        </p>
      </section>

      {/* Deliberately not wrapped in a `disabled` fieldset, unlike the
          ZooKeeper section it replaces. `disabled` locks connection
          *identity* while a cluster is connected — this notice reads no
          draft state and mutates none, and its only button opens a static
          documentation URL. Same reasoning as the Publishing checkbox. */}
      {isKRaftOnly(draft.kafkaVersion) ? (
        <KRaftNotice />
      ) : (
        <fieldset disabled={disabled} className="connection-modal-fieldset">
          <section className="connection-modal-section">
            <h3>Zookeeper</h3>
            <label className="connection-modal-checkbox-label">
              <input
                type="checkbox"
                checked={draft.zookeeperEnabled}
                onChange={(e) => onChange({ zookeeperEnabled: e.target.checked })}
              />
              Enable Zookeeper
            </label>
            {draft.zookeeperEnabled && (
              <>
                <label>
                  Zookeeper host
                  <div className="connection-modal-input-row">
                    <input
                      value={draft.zookeeperHost}
                      onChange={(e) => onChange({ zookeeperHost: e.target.value })}
                    />
                    <button
                      type="button"
                      aria-label="Ping zookeeper"
                      disabled={
                        pingZookeeper.isPending ||
                        draft.zookeeperHost.trim().length === 0 ||
                        draft.zookeeperPort.trim().length === 0
                      }
                      onClick={() =>
                        pingZookeeper.mutate({
                          host: draft.zookeeperHost.trim(),
                          port: Number(draft.zookeeperPort),
                        })
                      }
                    >
                      Ping
                    </button>
                  </div>
                </label>
                <PingResult mutation={pingZookeeper} failureMessage="Unable to reach zookeeper" />
                <label>
                  Zookeeper port
                  <input
                    inputMode="numeric"
                    value={draft.zookeeperPort}
                    onChange={(e) => onChange({ zookeeperPort: e.target.value })}
                  />
                </label>
                <label>
                  Zookeeper chroot path
                  <input
                    value={draft.zookeeperChrootPath}
                    onChange={(e) => onChange({ zookeeperChrootPath: e.target.value })}
                    placeholder="/kafka"
                  />
                </label>
              </>
            )}
          </section>
        </fieldset>
      )}
    </div>
  );
}
