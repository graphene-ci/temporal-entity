package wire

import (
	"errors"
	"fmt"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/graphene-ci/temporal-entity/pkg/entity"
)

func TestRecordCompletedEvictsOldest(t *testing.T) {
	env := &Envelope[struct{}, struct{}]{}
	for i := 0; i < CompletedOpsCap+5; i++ {
		env.RecordCompleted(entity.RequestID(fmt.Sprintf("req-%d", i)), []byte(`"ok"`), nil)
	}
	if len(env.Completed) != CompletedOpsCap || len(env.CompletedOrder) != CompletedOpsCap {
		t.Fatalf("cache not bounded: %d/%d", len(env.Completed), len(env.CompletedOrder))
	}
	if _, ok := env.Completed["req-0"]; ok {
		t.Fatal("oldest entry not evicted")
	}
	if _, ok := env.Completed[entity.RequestID(fmt.Sprintf("req-%d", CompletedOpsCap+4))]; !ok {
		t.Fatal("newest entry missing")
	}
}

func TestRecordCompletedOverwriteKeepsOrder(t *testing.T) {
	env := &Envelope[struct{}, struct{}]{}
	env.RecordCompleted("dup", nil, errors.New("first"))
	env.RecordCompleted("dup", []byte(`"second"`), nil)
	if len(env.CompletedOrder) != 1 {
		t.Fatalf("duplicate request id duplicated in order: %v", env.CompletedOrder)
	}
	if env.Completed["dup"].Err != "" {
		t.Fatal("overwrite did not replace the result")
	}
}

func TestMergeLabels(t *testing.T) {
	env := &Envelope[struct{}, struct{}]{}
	if !env.MergeLabels(map[string]string{"env": "prod", "team": "ci"}) {
		t.Fatal("first merge reported no change")
	}
	if env.MergeLabels(map[string]string{"env": "prod"}) {
		t.Fatal("identical merge reported a change")
	}
	if !env.MergeLabels(map[string]string{"team": ""}) {
		t.Fatal("delete reported no change")
	}
	if _, ok := env.Labels["team"]; ok {
		t.Fatal("empty value did not delete the key")
	}
	if env.MergeLabels(map[string]string{"team": ""}) {
		t.Fatal("deleting an absent key reported a change")
	}
}

// A refusal's code survives the cache: the message alone is kept as Err,
// the code as ErrType; a plain error stays text.
func TestRecordCompletedKeepsRefusalCode(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		err      error
		wantErr  string
		wantType string
	}{
		{name: "refusal", err: temporal.NewNonRetryableApplicationError("leased by r1", "busy", nil), wantErr: "leased by r1", wantType: "busy"},
		{name: "wrapped refusal", err: fmt.Errorf("acquire: %w", temporal.NewApplicationError("leaks", "dirty")), wantErr: "leaks", wantType: "dirty"},
		{name: "plain error", err: errors.New("boom"), wantErr: "boom"},
		{name: "untyped application error", err: temporal.NewApplicationError("x", ""), wantErr: "x"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			var e Envelope[struct{}, struct{}]
			e.RecordCompleted("req", nil, c.err)
			got := e.Completed["req"]
			if got.ErrType != c.wantType || (c.wantType != "" && got.Err != c.wantErr) {
				t.Fatalf("got %+v, want err %q type %q", got, c.wantErr, c.wantType)
			}
		})
	}
}
