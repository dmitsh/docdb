package queries

import (
	"fmt"
)

type Visitor interface {
	VisitEQ(*FilterEQ) (string, error)
	VisitIN(*FilterIN) (string, error)
	VisitAND(*FilterAND) (string, error)
	VisitOR(*FilterOR) (string, error)
	Finalize(string, *MidQuery) error
}

type QueryBuilder struct {
	visitor Visitor
}

func NewQueryBuilder(visitor Visitor) *QueryBuilder {
	return &QueryBuilder{
		visitor: visitor,
	}
}

func (h *QueryBuilder) BuildQuery(mq *MidQuery) error {
	filters, err := h.buildFilter(mq.Filter)
	if err != nil {
		return err
	}
	return h.visitor.Finalize(filters, mq)
}

func (h *QueryBuilder) buildFilter(filter Filter) (string, error) {
	if filter == nil {
		return "", nil
	}
	switch f := filter.(type) {
	case *FilterEQ:
		return h.visitor.VisitEQ(f)
	case *FilterIN:
		return h.visitor.VisitIN(f)
	case *FilterOR:
		return h.visitor.VisitOR(f)
	case *FilterAND:
		return h.visitor.VisitAND(f)
	default:
		return "", fmt.Errorf("Unsupported filter type %#v", filter)
	}
}
