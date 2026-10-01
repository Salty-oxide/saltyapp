import { useMemo } from "react";
import { Dropdown } from "../../../components/Dropdown";
import { KAFKA_VERSIONS } from "../../../lib/tauri";
import { usePingBootstrapServers, usePingZookeeper } from "../useConnections";
import { ConnectionDraft } from "./draft";
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
          <Dropdown
            label="Kafka cluster version"
            ariaLabel="Kafka cluster version"
            options={kafkaVersionOptions}
            displayedId={draft.kafkaVersion}
            appliedId={draft.kafkaVersion}
            onCommit={(id) => onChange({ kafkaVersion: id })}
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
    </div>
  );
}
