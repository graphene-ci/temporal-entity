package wire

import (
	"errors"
	"fmt"
	"testing"

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
