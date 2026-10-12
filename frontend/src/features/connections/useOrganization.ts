import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, FolderKind, Organization } from "../../lib/tauri";

const EMPTY: Organization = { folders: [], assignments: [], pinnedTopics: [] };

export const organizationKey = (connectionId: string) => ["organization", connectionId] as const;

/**
 * A connection's pins and folders. Local data (SQLite), so it is available
 * whether or not the cluster is reachable, and never retried or refetched on
 * focus — it only changes through the mutations below.
 */
export function useOrganization(connectionId: string) {
  const query = useQuery({
    queryKey: organizationKey(connectionId),
    queryFn: () => api.getOrganization(connectionId),
    staleTime: Infinity,
  });
  return { ...query, organization: query.data ?? EMPTY };
}

/** Every write refreshes the one query, so the tree always shows what is stored. */
export function useOrganizationActions(connectionId: string) {
  const queryClient = useQueryClient();
  const refresh = () => queryClient.invalidateQueries({ queryKey: organizationKey(connectionId) });

  const create = useMutation({
    mutationFn: ({ kind, name }: { kind: FolderKind; name: string }) => api.createFolder(connectionId, kind, name),
    onSuccess: refresh,
  });
  const rename = useMutation({
    mutationFn: ({ id, name }: { id: string; name: string }) => api.renameFolder(id, name),
    onSuccess: refresh,
  });
  const remove = useMutation({
    mutationFn: (id: string) => api.deleteFolder(id),
    onSuccess: refresh,
  });
  const move = useMutation({
    mutationFn: ({ kind, name, folderId }: { kind: FolderKind; name: string; folderId: string | null }) =>
      api.moveResource(connectionId, kind, name, folderId),
    onSuccess: refresh,
  });
  const pin = useMutation({
    mutationFn: ({ topic, pinned }: { topic: string; pinned: boolean }) =>
      api.setTopicPinned(connectionId, topic, pinned),
    onSuccess: refresh,
  });

  return {
    createFolder: (kind: FolderKind, name: string) => create.mutateAsync({ kind, name }),
    renameFolder: (id: string, name: string) => rename.mutateAsync({ id, name }),
    deleteFolder: (id: string) => remove.mutateAsync(id),
    moveResource: (kind: FolderKind, name: string, folderId: string | null) =>
      move.mutateAsync({ kind, name, folderId }),
    setPinned: (topic: string, pinned: boolean) => pin.mutateAsync({ topic, pinned }),
  };
}
