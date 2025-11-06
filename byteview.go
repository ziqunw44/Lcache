package Lcache

type ByteView struct {
	b []byte
}

func (b ByteView) Len() int {
	return len(b.b)
}