package entdefine

import (
	"strings"
	"testing"

	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/temporal-entity/pkg/entity"
)

type tSpec struct{}
type tState struct{}
type tRes struct{}

type cmdOK struct{}

func (cmdOK) Name() entity.CommandName { return "ok" }
func (cmdOK) Result() tRes             { return tRes{} }

type cmdDup struct{}

func (cmdDup) Name() entity.CommandName { return "ok" }
func (cmdDup) Result() tRes             { return tRes{} }

type cmdReservedQuery struct{}

func (cmdReservedQuery) Name() entity.CommandName { return "describe" }
func (cmdReservedQuery) Result() tRes             { return tRes{} }

type cmdReservedSignal struct{}

func (cmdReservedSignal) Name() entity.CommandName { return "entity-delete" }
func (cmdReservedSignal) Result() tRes             { return tRes{} }

type cmdEmpty struct{}

func (cmdEmpty) Name() entity.CommandName { return "" }
func (cmdEmpty) Result() tRes             { return tRes{} }

type qryOK struct{}

func (qryOK) Name() entity.QueryName { return "status" }
func (qryOK) Result() tRes           { return tRes{} }

type qryDup struct{}

func (qryDup) Name() entity.QueryName { return "status" }
func (qryDup) Result() tRes           { return tRes{} }

type qryReserved struct{}

func (qryReserved) Name() entity.QueryName { return "describe" }
func (qryReserved) Result() tRes           { return tRes{} }

func handlerFor[Req entity.Command[tRes]]() func(workflow.Context, *Ctx[tSpec, tState], Req) (tRes, error) {
	return func(workflow.Context, *Ctx[tSpec, tState], Req) (tRes, error) { return tRes{}, nil }
}

type fakeRegistry struct{ registered []string }

func (f *fakeRegistry) RegisterWorkflow(any) {}
func (f *fakeRegistry) RegisterWorkflowWithOptions(_ any, opts workflow.RegisterOptions) {
	f.registered = append(f.registered, opts.Name)
}
func (f *fakeRegistry) RegisterDynamicWorkflow(any, workflow.DynamicRegisterOptions) {}

func TestRegisterAccumulatesErrors(t *testing.T) {
	d := New[tSpec, tState]("kind")
	Handle(d, handlerFor[cmdOK]())
	Handle(d, handlerFor[cmdDup]())            // duplicate name
	Handle(d, handlerFor[cmdReservedQuery]())  // reserved query name
	Handle(d, handlerFor[cmdReservedSignal]()) // reserved signal name
	Handle(d, handlerFor[cmdEmpty]())          // empty name
	HandleQuery(d, func(entity.Snapshot[tSpec, tState], qryOK) (tRes, error) { return tRes{}, nil })
	HandleQuery(d, func(entity.Snapshot[tSpec, tState], qryDup) (tRes, error) { return tRes{}, nil })
	HandleQuery(d, func(entity.Snapshot[tSpec, tState], qryReserved) (tRes, error) { return tRes{}, nil })

	err := d.Register(&fakeRegistry{})
	if err == nil {
		t.Fatal("expected registration errors")
	}
	for _, want := range []string{"duplicate command", "reserved or empty command name", "duplicate query", "reserved or empty query name"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error list missing %q:\n%v", want, err)
		}
	}
}

func TestRegisterEmptyKind(t *testing.T) {
	d := New[tSpec, tState]("")
	if err := d.Register(&fakeRegistry{}); err == nil {
		t.Fatal("empty kind accepted")
	}
}

func TestRegisterSuccessAndIntrospection(t *testing.T) {
	d := New[tSpec, tState]("kind")
	Handle(d, handlerFor[cmdOK]())
	HandleQuery(d, func(entity.Snapshot[tSpec, tState], qryOK) (tRes, error) { return tRes{}, nil })

	reg := &fakeRegistry{}
	if err := d.Register(reg); err != nil {
		t.Fatalf("register: %v", err)
	}
	if len(reg.registered) != 1 || reg.registered[0] != "kind" {
		t.Fatalf("workflow not registered under kind name: %v", reg.registered)
	}
	if d.Kind() != "kind" {
		t.Fatalf("Kind() = %q", d.Kind())
	}

	cmds := d.Commands()
	if len(cmds) != 1 || cmds[0].Name != "ok" || cmds[0].ReqType == "" || cmds[0].ResType == "" {
		t.Fatalf("Commands() = %+v", cmds)
	}
	qs := d.Queries()
	if len(qs) != 1 || qs[0].Name != "status" || qs[0].ReqType == "" || qs[0].ResType == "" {
		t.Fatalf("Queries() = %+v", qs)
	}
}

func TestDeadRegistrationValidateIsNoop(_ *testing.T) {
	d := New[tSpec, tState]("kind")
	Handle(d, handlerFor[cmdOK]())
	// Duplicate registration is dead; attaching a validator must not panic.
	Handle(d, handlerFor[cmdDup]()).Validate(func(entity.Snapshot[tSpec, tState], cmdDup) error { return nil })
}
