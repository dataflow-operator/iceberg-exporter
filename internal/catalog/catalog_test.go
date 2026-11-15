package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNamespaceSelector(t *testing.T) {
	t.Parallel()
	sel := NamespaceSelector{Include: []string{"analytics"}, Exclude: []string{"analytics.secret"}}
	cases := []struct {
		ns   string
		want bool
	}{
		{ns: "analytics", want: true},
		{ns: "analytics.daily", want: true},
		{ns: "analytics.secret", want: false},
		{ns: "analytics.secret.raw", want: false},
		{ns: "analytic", want: false},
		{ns: "ops", want: false},
		{ns: "", want: false},
	}
	for _, tc := range cases {
		if got := sel.Allows(tc.ns); got != tc.want {
			t.Fatalf("Allows(%q) = %v, want %v", tc.ns, got, tc.want)
		}
	}
	open := NamespaceSelector{}
	if !open.Allows("ops") || !open.Allows("") {
		t.Fatal("empty selector allows every namespace")
	}
	if (NamespaceSelector{Exclude: []string{""}}).Allows("") {
		t.Fatal("excluding the empty namespace rejects it")
	}
}

func TestJoinNamespace(t *testing.T) {
	t.Parallel()
	if got := JoinNamespace("analytics", "daily"); got != "analytics.daily" {
		t.Fatalf("got %q", got)
	}
}

func TestTableUUIDAdoption(t *testing.T) {
	t.Parallel()
	raw := json.RawMessage(`{"format-version":2,"table-uuid":"abc-1","location":"s3://wh/t"}`)
	if got := tableUUID(raw); got != "abc-1" {
		t.Fatalf("got %q", got)
	}
	if got := tableUUID(json.RawMessage(`{"table-uuid":"  uuid-2  "}`)); got != "uuid-2" {
		t.Fatalf("got %q", got)
	}
	if tableUUID(json.RawMessage(`not-json`)) != "" {
		t.Fatal("invalid metadata has no uuid")
	}
	p := TablePointer{Metadata: raw}
	adoptIdentity(&p)
	if p.ContentID != "abc-1" {
		t.Fatalf("content id %q", p.ContentID)
	}
	p.ContentID = "nessie-1"
	p.Metadata = json.RawMessage(`{"table-uuid":"other"}`)
	adoptIdentity(&p)
	if p.ContentID != "nessie-1" {
		t.Fatalf("catalog content id was replaced: %q", p.ContentID)
	}
}

func TestGenerationChanged(t *testing.T) {
	t.Parallel()
	base := TablePointer{MetadataLocation: "s3://wh/v1.metadata.json", SnapshotID: snap(1), Type: ContentTable}
	if generationChanged(base, base) {
		t.Fatal("identical pointer is the same generation")
	}
	moved := base
	moved.MetadataLocation = "s3://wh/v2.metadata.json"
	moved.SnapshotID = snap(1)
	if !generationChanged(base, moved) {
		t.Fatal("metadata location is the primary signal")
	}
	resnap := base
	resnap.SnapshotID = snap(2)
	if !generationChanged(base, resnap) {
		t.Fatal("snapshot id is the secondary signal")
	}
	missing := base
	missing.SnapshotID = nil
	if generationChanged(base, missing) {
		t.Fatal("a missing snapshot id does not count as a new generation")
	}
	asView := base
	asView.Type = ContentView
	if !generationChanged(base, asView) {
		t.Fatal("content type change is a new generation")
	}
}

func TestDivergedTables(t *testing.T) {
	t.Parallel()
	base := []TablePointer{
		{ContentID: "a", Namespace: "db", Name: "old", SnapshotID: snap(1), Type: ContentTable},
		{ContentID: "b", Namespace: "db", Name: "kept", SnapshotID: snap(3), Type: ContentTable},
		{ContentID: "only-base", Namespace: "db", Name: "base", SnapshotID: snap(1), Type: ContentTable},
		{Namespace: "db", Name: "view", Type: ContentView, SnapshotID: snap(9)},
	}
	other := []TablePointer{
		{ContentID: "a", Namespace: "db", Name: "renamed", SnapshotID: snap(1), Type: ContentTable},
		{ContentID: "b", Namespace: "db", Name: "kept", SnapshotID: snap(4), Type: ContentTable},
		{ContentID: "c", Namespace: "db", Name: "new", SnapshotID: snap(1), Type: ContentTable},
		{Namespace: "db", Name: "unknown", Type: ContentTable},
		{Namespace: "db", Name: "same-unknown", Type: ContentTable},
		{Namespace: "db", Name: "view", Type: ContentView, SnapshotID: snap(8)},
	}
	base = append(base, TablePointer{Namespace: "db", Name: "same-unknown", Type: ContentTable})
	got := namesOf(DivergedTables(base, other))
	want := []string{"db.kept", "db.new", "db.unknown"}
	if join(got) != join(want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestRefresh_RevisionMatchSkipsList(t *testing.T) {
	t.Parallel()
	cat := newRevCatalog("rest")
	ref := refMain()
	cat.m.revisions[refKey(ref)] = Revision{Token: "aaa", CommitTime: time.Unix(10, 0).UTC()}
	cat.m.listFn = pageOf(tablePtr("db", "events", "s3://wh/v1.json", 1))
	state := NewState()
	now := time.Unix(100, 0).UTC()

	first, err := Refresh(context.Background(), cat, ref, state, Policy{Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if first.Mode != ModeList || cat.m.calls().list != 1 || cat.m.calls().rev != 1 {
		t.Fatalf("cold start mode %s list %d rev %d", first.Mode, cat.m.calls().list, cat.m.calls().rev)
	}

	second, err := Refresh(context.Background(), cat, ref, state, Policy{Now: now.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if second.Mode != ModeCache || cat.m.calls().list != 1 || cat.m.calls().diff != 0 {
		t.Fatalf("mode %s list %d diff %d", second.Mode, cat.m.calls().list, cat.m.calls().diff)
	}
	if !second.CheckedAt.Equal(now.Add(time.Minute)) || !second.ListedAt.Equal(now) {
		t.Fatalf("checked %s listed %s", second.CheckedAt, second.ListedAt)
	}
	if !second.Revision.CommitTime.Equal(time.Unix(10, 0).UTC()) {
		t.Fatalf("commit time %s", second.Revision.CommitTime)
	}
	if second.Reused != 1 || len(second.Changed) != 0 {
		t.Fatalf("reused %d changed %d", second.Reused, len(second.Changed))
	}
	if _, isRev := any(cat).(Revisioned); !isRev || cat.Type() != "rest" {
		t.Fatal("short-circuit comes from the capability, the type string is rest")
	}
}

func TestRefresh_PlainCatalogIgnoresTypeString(t *testing.T) {
	t.Parallel()
	cat := newListCatalog("nessie")
	ref := refMain()
	cat.m.listFn = pageOf(tablePtr("db", "events", "s3://wh/v1.json", 1))
	state := NewState()
	if _, ok := any(cat).(Revisioned); ok {
		t.Fatal("list catalog must not advertise revision")
	}
	for i := 0; i < 2; i++ {
		res, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(int64(i), 0).UTC()})
		if err != nil {
			t.Fatal(err)
		}
		if res.Mode != ModeList {
			t.Fatalf("refresh %d mode %s", i, res.Mode)
		}
	}
	if cat.m.calls().list != 2 {
		t.Fatalf("list calls %d", cat.m.calls().list)
	}
}

func TestRefresh_RevisionChangeWithoutDiffComparesMetadataLocation(t *testing.T) {
	t.Parallel()
	cat := newRevCatalog("glue")
	ref := refMain()
	state := NewState()
	events := tablePtr("db", "events", "s3://wh/events-v1.json", 1)
	users := tablePtr("db", "users", "s3://wh/users-v1.json", 4)
	cat.m.revisions[refKey(ref)] = Revision{Token: "aaa"}
	cat.m.listFn = pageOf(events, users)
	if _, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(1, 0).UTC()}); err != nil {
		t.Fatal(err)
	}

	cat.m.revisions[refKey(ref)] = Revision{Token: "bbb"}
	events.MetadataLocation = "s3://wh/events-v2.json"
	events.SnapshotID = snap(1)
	cat.m.listFn = pageOf(events, users)
	res, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(2, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != ModeList || cat.m.calls().list != 2 {
		t.Fatalf("mode %s list %d", res.Mode, cat.m.calls().list)
	}
	if names := namesOf(res.Changed); join(names) != "db.events" {
		t.Fatalf("changed %v", names)
	}
	if names := namesOf(res.FetchMetadata); join(names) != "db.events" {
		t.Fatalf("fetch %v", names)
	}
	if res.Reused != 1 {
		t.Fatalf("reused %d", res.Reused)
	}
}

func TestRefresh_SnapshotIDSecondary(t *testing.T) {
	t.Parallel()
	cat := newListCatalog("rest")
	ref := refMain()
	state := NewState()
	p := tablePtr("db", "events", "s3://wh/v1.json", 1)
	cat.m.listFn = pageOf(p)
	if _, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(1, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	p.SnapshotID = snap(2)
	cat.m.listFn = pageOf(p)
	res, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(2, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Changed) != 1 || res.Reused != 0 {
		t.Fatalf("changed %d reused %d", len(res.Changed), res.Reused)
	}
}

func TestRefresh_EmbeddedMetadataSkipsFetch(t *testing.T) {
	t.Parallel()
	cat := newListCatalog("rest")
	ref := refMain()
	p := tablePtr("db", "events", "s3://wh/v1.json", 1)
	p.Metadata = json.RawMessage(`{"table-uuid":"uuid-1","format-version":2}`)
	cat.m.listFn = pageOf(p, TablePointer{
		Namespace: "db", Name: "orders_view", MetadataLocation: "s3://wh/view.json", Type: ContentView,
	})
	res, err := Refresh(context.Background(), cat, ref, NewState(), Policy{Now: time.Unix(1, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Tables) != 1 || len(res.Views) != 1 {
		t.Fatalf("tables %d views %d", len(res.Tables), len(res.Views))
	}
	if res.Tables[0].ContentID != "uuid-1" {
		t.Fatalf("content id %q", res.Tables[0].ContentID)
	}
	if len(res.FetchMetadata) != 0 {
		t.Fatalf("fetch %v", namesOf(res.FetchMetadata))
	}
	if len(res.Changed) != 2 {
		t.Fatalf("changed %v", namesOf(res.Changed))
	}
}

func TestRefresh_RekeyFromNameToContentID(t *testing.T) {
	t.Parallel()
	cat := newListCatalog("hive")
	ref := refMain()
	state := NewState()
	now := time.Unix(1, 0).UTC()
	first := tablePtr("db", "events", "s3://wh/v1.json", 1)
	cat.m.listFn = pageOf(first)
	if _, err := Refresh(context.Background(), cat, ref, state, Policy{Now: now}); err != nil {
		t.Fatal(err)
	}

	second := first
	second.Metadata = json.RawMessage(`{"table-uuid":"uuid-9"}`)
	cat.m.listFn = pageOf(second)
	res, err := Refresh(context.Background(), cat, ref, state, Policy{Now: now.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Removed) != 0 || len(res.Tables) != 1 || res.Tables[0].ContentID != "uuid-9" || res.Reused != 1 {
		t.Fatalf("removed %v tables %+v reused %d", namesOf(res.Removed), res.Tables, res.Reused)
	}

	renamed := second
	renamed.Name = "events_v2"
	cat.m.listFn = pageOf(renamed)
	res, err = Refresh(context.Background(), cat, ref, state, Policy{Now: now.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Removed) != 0 || len(res.Tables) != 1 || res.Tables[0].Name != "events_v2" || res.Reused != 1 {
		t.Fatalf("removed %v tables %+v reused %d", namesOf(res.Removed), res.Tables, res.Reused)
	}
}

func TestRefresh_ReplacedContentIDRemovesPreviousTable(t *testing.T) {
	t.Parallel()
	cat := newListCatalog("hive")
	ref := refMain()
	state := NewState()
	old := tablePtr("db", "events", "s3://wh/v1.json", 1)
	old.ContentID = "id-old"
	cat.m.listFn = pageOf(old)
	if _, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(1, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	next := tablePtr("db", "events", "s3://wh/v2.json", 2)
	next.ContentID = "id-new"
	cat.m.listFn = pageOf(next)
	res, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(2, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Tables) != 1 || res.Tables[0].ContentID != "id-new" {
		t.Fatalf("tables %+v", res.Tables)
	}
	if len(res.Removed) != 1 || res.Removed[0].ContentID != "id-old" {
		t.Fatalf("removed %+v", res.Removed)
	}
}

func TestState_BindMetadataRekeysAndSurvivesLaterList(t *testing.T) {
	t.Parallel()
	cat := newListCatalog("hive")
	ref := refMain()
	state := NewState()
	p := tablePtr("db", "events", "s3://wh/v1.json", 1)
	cat.m.listFn = pageOf(p)
	if _, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(1, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	meta := json.RawMessage(`{"table-uuid":"uuid-bind"}`)
	bound, ok := state.BindMetadata(ref, "db", "events", meta)
	if !ok || bound.ContentID != "uuid-bind" {
		t.Fatalf("bound %+v ok %v", bound, ok)
	}
	if _, ok := state.BindMetadata(ref, "db", "missing", meta); ok {
		t.Fatal("missing table was bound")
	}

	cat.m.listFn = pageOf(p)
	res, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(2, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Tables) != 1 || res.Tables[0].ContentID != "uuid-bind" || len(res.Removed) != 0 || res.Reused != 1 {
		t.Fatalf("tables %+v removed %d reused %d", res.Tables, len(res.Removed), res.Reused)
	}

	withID := p
	withID.ContentID = "catalog-id"
	cat.m.listFn = pageOf(withID)
	if _, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(3, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	kept, ok := state.BindMetadata(ref, "db", "events", json.RawMessage(`{"table-uuid":"other-uuid"}`))
	if !ok || kept.ContentID != "catalog-id" {
		t.Fatalf("content id %q ok %v", kept.ContentID, ok)
	}
}

func TestRefresh_ListIntervalServesCacheUntilElapsed(t *testing.T) {
	t.Parallel()
	cat := newListCatalog("rest")
	ref := refMain()
	state := NewState()
	p := tablePtr("db", "events", "s3://wh/v1.json", 1)
	cat.m.listFn = pageOf(p)
	start := time.Unix(1_000, 0).UTC()
	policy := Policy{Now: start, ListInterval: time.Minute}
	first, err := Refresh(context.Background(), cat, ref, state, policy)
	if err != nil {
		t.Fatal(err)
	}
	policy.Now = start.Add(30 * time.Second)
	second, err := Refresh(context.Background(), cat, ref, state, policy)
	if err != nil {
		t.Fatal(err)
	}
	if second.Mode != ModeCache || cat.m.calls().list != 1 || !second.CheckedAt.Equal(first.CheckedAt) {
		t.Fatalf("mode %s list %d checked %s", second.Mode, cat.m.calls().list, second.CheckedAt)
	}

	p.MetadataLocation = "s3://wh/v2.json"
	cat.m.listFn = pageOf(p)
	policy.Now = start.Add(time.Minute)
	third, err := Refresh(context.Background(), cat, ref, state, policy)
	if err != nil {
		t.Fatal(err)
	}
	if third.Mode != ModeList || cat.m.calls().list != 2 || join(namesOf(third.Changed)) != "db.events" {
		t.Fatalf("mode %s list %d changed %v", third.Mode, cat.m.calls().list, namesOf(third.Changed))
	}
}

func TestRefresh_DiffAppliesAndEmptyDiffAdvancesToken(t *testing.T) {
	t.Parallel()
	cat := newDiffCatalog("nessie")
	ref := refMain()
	state := NewState()
	kept := tablePtr("db", "kept", "s3://wh/kept-v1.json", 1)
	kept.ContentID = "kept"
	moving := tablePtr("db", "moving", "s3://wh/moving-v1.json", 2)
	moving.ContentID = "moving"
	cat.m.revisions[refKey(ref)] = Revision{Token: "aaa", CommitTime: time.Unix(10, 0).UTC()}
	cat.m.listFn = pageOf(kept, moving)
	if _, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(100, 0).UTC()}); err != nil {
		t.Fatal(err)
	}

	updated := moving
	updated.MetadataLocation = "s3://wh/moving-v2.json"
	updated.SnapshotID = snap(3)
	same := kept
	cat.m.revisions[refKey(ref)] = Revision{Token: "bbb", CommitTime: time.Unix(20, 0).UTC()}
	cat.m.diffFn = func(Ref, Revision, Revision) ([]Change, error) {
		return []Change{
			{Kind: ChangeUpsert, Pointer: updated},
			{Kind: ChangeUpsert, Pointer: same},
			{Kind: ChangeDelete, Pointer: TablePointer{ContentID: "missing"}},
			{Kind: ChangeUpsert, Pointer: tablePtr("db", "added", "s3://wh/added.json", 1)},
		}, nil
	}
	res, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(120, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != ModeDiff || cat.m.calls().list != 1 || cat.m.calls().diff != 1 {
		t.Fatalf("mode %s list %d diff %d", res.Mode, cat.m.calls().list, cat.m.calls().diff)
	}
	if join(namesOf(res.Changed)) != "db.added,db.moving" {
		t.Fatalf("changed %v", namesOf(res.Changed))
	}
	if join(namesOf(res.FetchMetadata)) != "db.added,db.moving" || res.Reused != 1 {
		t.Fatalf("fetch %v reused %d", namesOf(res.FetchMetadata), res.Reused)
	}
	if !res.ListedAt.Equal(time.Unix(100, 0).UTC()) || !res.CheckedAt.Equal(time.Unix(120, 0).UTC()) {
		t.Fatalf("listed %s checked %s", res.ListedAt, res.CheckedAt)
	}
	if from, to := cat.m.diffTokens(); from != "aaa" || to != "bbb" {
		t.Fatalf("diff %s -> %s", from, to)
	}

	cat.m.revisions[refKey(ref)] = Revision{Token: "ccc", CommitTime: time.Unix(30, 0).UTC()}
	cat.m.diffFn = func(Ref, Revision, Revision) ([]Change, error) { return nil, nil }
	res, err = Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(130, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != ModeDiff || res.Revision.Token != "ccc" || len(res.Changed) != 0 || res.Reused != 3 {
		t.Fatalf("mode %s token %s changed %d reused %d", res.Mode, res.Revision.Token, len(res.Changed), res.Reused)
	}

	res, err = Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(140, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != ModeCache || cat.m.calls().diff != 2 || cat.m.calls().list != 1 {
		t.Fatalf("mode %s diff %d list %d", res.Mode, cat.m.calls().diff, cat.m.calls().list)
	}
}

func TestRefresh_IncompleteDiffFallsBackToList(t *testing.T) {
	t.Parallel()
	cat := newDiffCatalog("nessie")
	ref := refMain()
	state := NewState()
	p := tablePtr("db", "events", "s3://wh/v1.json", 1)
	p.ContentID = "id-1"
	cat.m.revisions[refKey(ref)] = Revision{Token: "aaa"}
	cat.m.listFn = pageOf(p)
	if _, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(1, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	cat.m.revisions[refKey(ref)] = Revision{Token: "bbb"}
	cat.m.diffFn = func(Ref, Revision, Revision) ([]Change, error) {
		return []Change{{Kind: ChangeUpsert, Pointer: TablePointer{ContentID: "id-2"}}}, nil
	}
	p.MetadataLocation = "s3://wh/v2.json"
	cat.m.listFn = pageOf(p)
	res, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(2, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != ModeList || cat.m.calls().list != 2 || cat.m.calls().diff != 1 {
		t.Fatalf("mode %s list %d diff %d", res.Mode, cat.m.calls().list, cat.m.calls().diff)
	}
	if res.Revision.Token != "bbb" || join(namesOf(res.Changed)) != "db.events" {
		t.Fatalf("token %s changed %v", res.Revision.Token, namesOf(res.Changed))
	}
}

func TestRefresh_DiffAndRevisionErrorsKeepCache(t *testing.T) {
	t.Parallel()
	cat := newDiffCatalog("nessie")
	ref := refMain()
	state := NewState()
	p := tablePtr("db", "events", "s3://wh/v1.json", 1)
	cat.m.revisions[refKey(ref)] = Revision{Token: "aaa"}
	cat.m.listFn = pageOf(p)
	if _, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(1, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	cat.m.revisions[refKey(ref)] = Revision{Token: "bbb"}
	cat.m.diffFn = func(Ref, Revision, Revision) ([]Change, error) {
		return nil, errors.New("diff down")
	}
	_, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(2, 0).UTC()})
	if err == nil || cat.m.calls().list != 1 {
		t.Fatalf("err %v list %d", err, cat.m.calls().list)
	}
	snap, ok := state.Snapshot(ref)
	if !ok || snap.Revision.Token != "aaa" {
		t.Fatalf("snapshot %+v ok %v", snap.Revision, ok)
	}

	cat.m.revErr = errors.New("hash down")
	_, err = Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(3, 0).UTC()})
	if err == nil || cat.m.calls().list != 1 {
		t.Fatalf("err %v list %d", err, cat.m.calls().list)
	}
	snap, _ = state.Snapshot(ref)
	if snap.Revision.Token != "aaa" || len(snap.Tables) != 1 {
		t.Fatalf("snapshot %+v tables %d", snap.Revision, len(snap.Tables))
	}
}

func TestRefresh_UnknownChangeKind(t *testing.T) {
	t.Parallel()
	cat := newDiffCatalog("nessie")
	ref := refMain()
	state := NewState()
	cat.m.revisions[refKey(ref)] = Revision{Token: "aaa"}
	cat.m.listFn = pageOf(tablePtr("db", "events", "s3://wh/v1.json", 1))
	if _, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(1, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	cat.m.revisions[refKey(ref)] = Revision{Token: "bbb"}
	cat.m.diffFn = func(Ref, Revision, Revision) ([]Change, error) {
		return []Change{{}}, nil
	}
	if _, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(2, 0).UTC()}); err == nil {
		t.Fatal("expected unknown kind error")
	}
	if cat.m.calls().list != 1 {
		t.Fatalf("list calls %d", cat.m.calls().list)
	}
}

func TestRefresh_ReconcileTimerListsInsteadOfDiff(t *testing.T) {
	t.Parallel()
	cat := newDiffCatalog("nessie")
	ref := refMain()
	state := NewState()
	p := tablePtr("db", "events", "s3://wh/v1.json", 1)
	cat.m.revisions[refKey(ref)] = Revision{Token: "aaa"}
	cat.m.listFn = pageOf(p)
	cat.m.diffFn = func(Ref, Revision, Revision) ([]Change, error) {
		return nil, errors.New("diff should not run")
	}
	start := time.Unix(1_000, 0).UTC()
	if _, err := Refresh(context.Background(), cat, ref, state, Policy{Now: start, ReconcileInterval: time.Hour}); err != nil {
		t.Fatal(err)
	}
	res, err := Refresh(context.Background(), cat, ref, state, Policy{Now: start.Add(time.Hour), ReconcileInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != ModeList || cat.m.calls().list != 2 || cat.m.calls().diff != 0 {
		t.Fatalf("mode %s list %d diff %d", res.Mode, cat.m.calls().list, cat.m.calls().diff)
	}

	cat.m.revisions[refKey(ref)] = Revision{Token: "bbb"}
	p.MetadataLocation = "s3://wh/v2.json"
	cat.m.listFn = pageOf(p)
	res, err = Refresh(context.Background(), cat, ref, state, Policy{Now: start.Add(2 * time.Hour), ReconcileInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != ModeList || cat.m.calls().diff != 0 || join(namesOf(res.Changed)) != "db.events" {
		t.Fatalf("mode %s diff %d changed %v", res.Mode, cat.m.calls().diff, namesOf(res.Changed))
	}
}

func TestRefresh_NamespaceFilterIsPushedDownAndRemovesCached(t *testing.T) {
	t.Parallel()
	cat := newListCatalog("rest")
	ref := refMain()
	state := NewState()
	cat.m.listFn = func(Ref, ListOptions) (TablePage, error) {
		return TablePage{Tables: []TablePointer{
			tablePtr("analytics", "events", "s3://wh/e.json", 1),
			tablePtr("analytics.secret", "raw", "s3://wh/s.json", 1),
			tablePtr("ops", "users", "s3://wh/u.json", 1),
		}}, nil
	}
	sel := NamespaceSelector{Include: []string{"analytics"}, Exclude: []string{"analytics.secret"}}
	res, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(1, 0).UTC(), Namespaces: sel})
	if err != nil {
		t.Fatal(err)
	}
	if join(namesOf(res.Tables)) != "analytics.events" {
		t.Fatalf("tables %v", namesOf(res.Tables))
	}
	if got := cat.m.lastSelector(); !sameSelector(got, sel) {
		t.Fatalf("selector %+v", got)
	}
	cat.m.listFn = pageOf(tablePtr("ops", "users", "s3://wh/u.json", 1))
	res, err = Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(2, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Tables) != 1 || res.Tables[0].Namespace != "ops" || join(namesOf(res.Removed)) != "analytics.events" {
		t.Fatalf("tables %v removed %v", namesOf(res.Tables), namesOf(res.Removed))
	}
}

func TestRefresh_PaginationAndRepeatedToken(t *testing.T) {
	t.Parallel()
	cat := newListCatalog("rest")
	ref := refMain()
	cat.m.listFn = func(_ Ref, opts ListOptions) (TablePage, error) {
		switch opts.PageToken {
		case "":
			return TablePage{Tables: []TablePointer{tablePtr("db", "a", "s3://wh/a.json", 1)}, NextPageToken: "p2"}, nil
		case "p2":
			return TablePage{Tables: []TablePointer{tablePtr("db", "b", "s3://wh/b.json", 1)}}, nil
		default:
			return TablePage{}, errors.New("unexpected token")
		}
	}
	res, err := Refresh(context.Background(), cat, ref, NewState(), Policy{Now: time.Unix(1, 0).UTC(), PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if join(namesOf(res.Tables)) != "db.a,db.b" || cat.m.calls().list != 2 || cat.m.lastPageSize() != 1 {
		t.Fatalf("tables %v calls %d", namesOf(res.Tables), cat.m.calls().list)
	}

	cat.m.listFn = func(_ Ref, opts ListOptions) (TablePage, error) {
		return TablePage{Tables: []TablePointer{tablePtr("db", "a", "s3://wh/a.json", 1)}, NextPageToken: "p2"}, nil
	}
	if _, err := Refresh(context.Background(), cat, ref, NewState(), Policy{Now: time.Unix(2, 0).UTC()}); err == nil {
		t.Fatal("expected repeated page token")
	}
}

func TestRefresh_ListErrorDoesNotPublishPartialPage(t *testing.T) {
	t.Parallel()
	cat := newListCatalog("rest")
	ref := refMain()
	state := NewState()
	cat.m.listFn = pageOf(tablePtr("db", "events", "s3://wh/v1.json", 1))
	if _, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(1, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cat.m.listFn = func(Ref, ListOptions) (TablePage, error) {
		cancel()
		return TablePage{Tables: []TablePointer{tablePtr("db", "other", "s3://wh/x.json", 1)}, NextPageToken: "more"}, nil
	}
	if _, err := Refresh(ctx, cat, ref, state, Policy{Now: time.Unix(2, 0).UTC()}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
	snap, ok := state.Snapshot(ref)
	if !ok || join(namesOf(snap.Tables)) != "db.events" {
		t.Fatalf("snapshot %v ok %v", namesOf(snap.Tables), ok)
	}
}

func TestRefresh_EmptyRevisionTokenDoesNotShortCircuit(t *testing.T) {
	t.Parallel()
	cat := newRevCatalog("nessie")
	ref := refMain()
	state := NewState()
	cat.m.revisions[refKey(ref)] = Revision{Token: "aaa"}
	cat.m.listFn = pageOf(tablePtr("db", "events", "s3://wh/v1.json", 1))
	if _, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(1, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	cat.m.revisions[refKey(ref)] = Revision{}
	res, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(2, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != ModeList || cat.m.calls().list != 2 || res.Revision.Token != "" {
		t.Fatalf("mode %s list %d token %q", res.Mode, cat.m.calls().list, res.Revision.Token)
	}
}

func TestRefresh_InputIsolation(t *testing.T) {
	t.Parallel()
	cat := newListCatalog("rest")
	ref := refMain()
	state := NewState()
	id := int64(7)
	meta := json.RawMessage(`{"table-uuid":"uuid-7"}`)
	p := tablePtr("db", "events", "s3://wh/v1.json", 0)
	p.SnapshotID = &id
	p.Metadata = meta
	cat.m.listFn = pageOf(p)
	res, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(1, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	id = 8
	meta[0] = 'x'
	if *res.Tables[0].SnapshotID != 7 || res.Tables[0].Metadata[0] != '{' {
		t.Fatalf("result alias snapshot %d meta %s", *res.Tables[0].SnapshotID, res.Tables[0].Metadata)
	}
	snap, ok := state.Snapshot(ref)
	if !ok || *snap.Tables[0].SnapshotID != 7 || snap.Tables[0].Metadata[0] != '{' {
		t.Fatalf("cache alias %+v", snap.Tables)
	}
}

func TestRefresh_SnapshotDoesNotWaitForList(t *testing.T) {
	t.Parallel()
	cat := newListCatalog("rest")
	ref := refMain()
	state := NewState()
	cat.m.listFn = pageOf(tablePtr("db", "events", "s3://wh/v1.json", 1))
	if _, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(1, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	cat.m.listFn = func(Ref, ListOptions) (TablePage, error) {
		close(entered)
		<-release
		return TablePage{Tables: []TablePointer{tablePtr("db", "events", "s3://wh/v2.json", 2)}}, nil
	}
	done := make(chan error, 1)
	go func() {
		_, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(2, 0).UTC()})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("list did not start")
	}
	start := time.Now()
	snap, ok := state.Snapshot(ref)
	if !ok || time.Since(start) > 500*time.Millisecond || join(namesOf(snap.Tables)) != "db.events" || *snap.Tables[0].SnapshotID != 1 {
		t.Fatalf("snapshot blocked or moved: ok %v tables %+v wait %s", ok, snap.Tables, time.Since(start))
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not finish")
	}
}

func TestRefreshAll_MultiRefDivergenceAndBaseFirst(t *testing.T) {
	t.Parallel()
	cat := newMultiCatalog("nessie")
	base := Ref{Name: "main", Kind: RefBranch}
	feature := Ref{Name: "feature", Kind: RefBranch}
	cat.m.refs = []Ref{feature, base, feature}
	cat.m.base = base
	cat.m.hasBase = true
	var listed []string
	cat.m.listFn = func(ref Ref, _ ListOptions) (TablePage, error) {
		listed = append(listed, ref.Name)
		switch ref.Name {
		case "main":
			return TablePage{Tables: []TablePointer{
				withID("a", tablePtr("db", "orders", "s3://wh/orders-v1.json", 1)),
				withID("b", tablePtr("db", "users", "s3://wh/users-v1.json", 1)),
				{Namespace: "db", Name: "orders_view", Type: ContentView},
			}}, nil
		case "feature":
			return TablePage{Tables: []TablePointer{
				withID("a", tablePtr("db", "orders", "s3://wh/orders-v2.json", 2)),
				withID("c", tablePtr("db", "new", "s3://wh/new.json", 1)),
				{Namespace: "db", Name: "orders_view", Type: ContentView, SnapshotID: snap(5)},
			}}, nil
		default:
			return TablePage{}, errors.New("unexpected ref")
		}
	}
	cat.m.revisions[refKey(base)] = Revision{Token: "main-1"}
	cat.m.revisions[refKey(feature)] = Revision{Token: "feature-1"}
	results, err := RefreshAll(context.Background(), cat, NewState(), Policy{Now: time.Unix(1, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if join(listed) != "main,feature" {
		t.Fatalf("list order %v", listed)
	}
	if len(results) != 2 || results[0].Ref.Name != "main" || results[0].BaseCompared {
		t.Fatalf("base result %+v", results[0].Ref)
	}
	featureRes := results[1]
	if !featureRes.BaseCompared || join(namesOf(featureRes.DivergedFromBase)) != "db.new,db.orders" {
		t.Fatalf("diverged %v compared %v", namesOf(featureRes.DivergedFromBase), featureRes.BaseCompared)
	}
}

func TestRefreshAll_StopsOnRefError(t *testing.T) {
	t.Parallel()
	cat := newRevCatalog("nessie")
	okRef := Ref{Name: "main", Kind: RefBranch}
	badRef := Ref{Name: "bad", Kind: RefBranch}
	cat.m.refs = []Ref{okRef, badRef}
	cat.m.revisions[refKey(okRef)] = Revision{Token: "aaa"}
	cat.m.revErrRefs = map[string]error{refKey(badRef): errors.New("missing ref")}
	cat.m.listFn = pageOf(tablePtr("db", "events", "s3://wh/v1.json", 1))
	results, err := RefreshAll(context.Background(), cat, NewState(), Policy{Now: time.Unix(1, 0).UTC()})
	if err == nil || len(results) != 1 || results[0].Ref.Name != "main" {
		t.Fatalf("err %v results %d", err, len(results))
	}
}

func TestRefresh_ForceFullListsDespiteMatchingToken(t *testing.T) {
	t.Parallel()
	cat := newDiffCatalog("nessie")
	ref := refMain()
	state := NewState()
	cat.m.revisions[refKey(ref)] = Revision{Token: "aaa"}
	cat.m.listFn = pageOf(tablePtr("db", "events", "s3://wh/v1.json", 1))
	cat.m.diffFn = func(Ref, Revision, Revision) ([]Change, error) {
		return nil, errors.New("diff should not run")
	}
	now := time.Unix(1, 0).UTC()
	if _, err := Refresh(context.Background(), cat, ref, state, Policy{Now: now}); err != nil {
		t.Fatal(err)
	}
	res, err := Refresh(context.Background(), cat, ref, state, Policy{Now: now.Add(time.Second), ForceFull: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != ModeList || cat.m.calls().list != 2 || cat.m.calls().diff != 0 || res.Reused != 1 {
		t.Fatalf("mode %s list %d diff %d reused %d", res.Mode, cat.m.calls().list, cat.m.calls().diff, res.Reused)
	}
}

func TestRefresh_RejectsNilAndEmptyRef(t *testing.T) {
	t.Parallel()
	if _, err := Refresh(context.Background(), nil, refMain(), NewState(), Policy{}); err == nil {
		t.Fatal("nil catalog")
	}
	if _, err := Refresh(context.Background(), newListCatalog("rest"), Ref{}, NewState(), Policy{}); err == nil {
		t.Fatal("empty ref")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Refresh(ctx, newListCatalog("rest"), refMain(), NewState(), Policy{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
}

func TestProbe(t *testing.T) {
	t.Parallel()
	cat := newListCatalog("rest")
	if err := cat.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	cat.m.probeErr = errors.New("down")
	if err := cat.Probe(context.Background()); err == nil || cat.m.calls().probe != 2 {
		t.Fatalf("err %v probes %d", err, cat.m.calls().probe)
	}
	if cat.Name() != "warehouse" || cat.Type() != "rest" {
		t.Fatalf("name %s type %s", cat.Name(), cat.Type())
	}
}

func TestRefresh_DiffRenameAndDelete(t *testing.T) {
	t.Parallel()
	cat := newDiffCatalog("nessie")
	ref := refMain()
	state := NewState()
	p := withID("id-1", tablePtr("db", "events", "s3://wh/v1.json", 1))
	cat.m.revisions[refKey(ref)] = Revision{Token: "aaa"}
	cat.m.listFn = pageOf(p)
	if _, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(1, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	renamed := p
	renamed.Name = "events2"
	cat.m.revisions[refKey(ref)] = Revision{Token: "bbb"}
	cat.m.diffFn = func(Ref, Revision, Revision) ([]Change, error) {
		return []Change{{Kind: ChangeUpsert, Pointer: renamed}}, nil
	}
	res, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(2, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Tables) != 1 || res.Tables[0].Name != "events2" || len(res.Removed) != 0 || res.Reused != 1 {
		t.Fatalf("tables %+v removed %d reused %d", res.Tables, len(res.Removed), res.Reused)
	}
	cat.m.revisions[refKey(ref)] = Revision{Token: "ccc"}
	cat.m.diffFn = func(Ref, Revision, Revision) ([]Change, error) {
		return []Change{{Kind: ChangeDelete, Pointer: TablePointer{ContentID: "id-1"}}}, nil
	}
	res, err = Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(3, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Tables) != 0 || len(res.Removed) != 1 || res.Removed[0].Name != "events2" {
		t.Fatalf("tables %v removed %+v", namesOf(res.Tables), res.Removed)
	}
}

func TestRefresh_DiffDropsNamespaceOutsideSelector(t *testing.T) {
	t.Parallel()
	cat := newDiffCatalog("nessie")
	ref := refMain()
	state := NewState()
	inside := withID("in", tablePtr("analytics", "events", "s3://wh/e.json", 1))
	cat.m.revisions[refKey(ref)] = Revision{Token: "aaa"}
	cat.m.listFn = pageOf(inside)
	sel := NamespaceSelector{Include: []string{"analytics"}}
	if _, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(1, 0).UTC(), Namespaces: sel}); err != nil {
		t.Fatal(err)
	}
	moved := inside
	moved.Namespace = "ops"
	cat.m.revisions[refKey(ref)] = Revision{Token: "bbb"}
	cat.m.diffFn = func(Ref, Revision, Revision) ([]Change, error) {
		return []Change{
			{Kind: ChangeUpsert, Pointer: moved},
			{Kind: ChangeUpsert, Pointer: withID("out", tablePtr("ops", "other", "s3://wh/o.json", 1))},
		}, nil
	}
	res, err := Refresh(context.Background(), cat, ref, state, Policy{Now: time.Unix(2, 0).UTC(), Namespaces: sel})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Tables) != 0 || join(namesOf(res.Removed)) != "analytics.events" {
		t.Fatalf("tables %v removed %v", namesOf(res.Tables), namesOf(res.Removed))
	}
}

type callCounts struct {
	list, rev, diff, probe int
}

type mem struct {
	mu         sync.Mutex
	name       string
	typ        string
	refs       []Ref
	base       Ref
	hasBase    bool
	probeErr   error
	revErr     error
	revErrRefs map[string]error
	revisions  map[string]Revision
	listFn     func(Ref, ListOptions) (TablePage, error)
	diffFn     func(Ref, Revision, Revision) ([]Change, error)

	listCalls  int
	revCalls   int
	diffCalls  int
	probeCalls int
	lastOpts   ListOptions
	fromToken  string
	toToken    string
}

type listCatalog struct{ m *mem }
type revCatalog struct{ listCatalog }
type diffCatalog struct{ revCatalog }
type multiCatalog struct{ diffCatalog }

func newListCatalog(typ string) *listCatalog {
	return &listCatalog{m: newMem(typ)}
}

func newRevCatalog(typ string) *revCatalog {
	return &revCatalog{listCatalog: listCatalog{m: newMem(typ)}}
}

func newDiffCatalog(typ string) *diffCatalog {
	return &diffCatalog{revCatalog: revCatalog{listCatalog: listCatalog{m: newMem(typ)}}}
}

func newMultiCatalog(typ string) *multiCatalog {
	return &multiCatalog{diffCatalog: diffCatalog{revCatalog: revCatalog{listCatalog: listCatalog{m: newMem(typ)}}}}
}

func newMem(typ string) *mem {
	return &mem{
		name:      "warehouse",
		typ:       typ,
		revisions: map[string]Revision{},
	}
}

func (c *listCatalog) Name() string { return c.m.name }
func (c *listCatalog) Type() string { return c.m.typ }

func (c *listCatalog) Probe(context.Context) error {
	c.m.mu.Lock()
	defer c.m.mu.Unlock()
	c.m.probeCalls++
	return c.m.probeErr
}

func (c *listCatalog) ListRefs(context.Context) ([]Ref, error) {
	c.m.mu.Lock()
	defer c.m.mu.Unlock()
	return append([]Ref(nil), c.m.refs...), nil
}

func (c *listCatalog) ListTables(ctx context.Context, ref Ref, opts ListOptions) (TablePage, error) {
	if err := ctx.Err(); err != nil {
		return TablePage{}, err
	}
	c.m.mu.Lock()
	c.m.listCalls++
	c.m.lastOpts = opts
	fn := c.m.listFn
	c.m.mu.Unlock()
	if fn == nil {
		return TablePage{}, nil
	}
	return fn(ref, opts)
}

func (c *revCatalog) Revision(ctx context.Context, ref Ref) (Revision, error) {
	if err := ctx.Err(); err != nil {
		return Revision{}, err
	}
	c.m.mu.Lock()
	defer c.m.mu.Unlock()
	c.m.revCalls++
	if err := c.m.revErrRefs[refKey(ref)]; err != nil {
		return Revision{}, err
	}
	if c.m.revErr != nil {
		return Revision{}, c.m.revErr
	}
	return c.m.revisions[refKey(ref)], nil
}

func (c *diffCatalog) Diff(ctx context.Context, ref Ref, from, to Revision) ([]Change, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.m.mu.Lock()
	c.m.diffCalls++
	c.m.fromToken = from.Token
	c.m.toToken = to.Token
	fn := c.m.diffFn
	c.m.mu.Unlock()
	if fn == nil {
		return nil, nil
	}
	return fn(ref, from, to)
}

func (c *multiCatalog) BaseRef() (Ref, bool) {
	return c.m.base, c.m.hasBase
}

func (m *mem) calls() callCounts {
	m.mu.Lock()
	defer m.mu.Unlock()
	return callCounts{list: m.listCalls, rev: m.revCalls, diff: m.diffCalls, probe: m.probeCalls}
}

func (m *mem) lastSelector() NamespaceSelector {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastOpts.Namespaces
}

func (m *mem) lastPageSize() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastOpts.PageSize
}

func (m *mem) diffTokens() (string, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fromToken, m.toToken
}

func pageOf(ps ...TablePointer) func(Ref, ListOptions) (TablePage, error) {
	copied := append([]TablePointer(nil), ps...)
	return func(Ref, ListOptions) (TablePage, error) {
		return TablePage{Tables: copied}, nil
	}
}

func tablePtr(namespace, name, location string, snapshot int64) TablePointer {
	return TablePointer{
		Namespace:        namespace,
		Name:             name,
		MetadataLocation: location,
		SnapshotID:       snap(snapshot),
		Type:             ContentTable,
	}
}

func withID(id string, p TablePointer) TablePointer {
	p.ContentID = id
	return p
}

func snap(id int64) *int64 { return &id }

func refMain() Ref { return Ref{Name: "main", Kind: RefBranch} }

func namesOf(ps []TablePointer) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.Namespace + "." + p.Name
	}
	return out
}

func join(ss []string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += ","
		}
		out += s
	}
	return out
}

func sameSelector(a, b NamespaceSelector) bool {
	return join(a.Include) == join(b.Include) && join(a.Exclude) == join(b.Exclude)
}

var (
	_ Catalog    = (*listCatalog)(nil)
	_ Catalog    = (*revCatalog)(nil)
	_ Revisioned = (*revCatalog)(nil)
	_ Diffable   = (*diffCatalog)(nil)
	_ MultiRef   = (*multiCatalog)(nil)
)
