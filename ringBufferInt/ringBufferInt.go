package ringBufferInt

type ringCell struct {
	value int
	next  *ringCell
}

type ringBufferInt struct {
	read         *ringCell
	write        *ringCell
	capacity     int
	currentCount int
}

func CreateRingBuffer(capacity int) *ringBufferInt {
	if capacity <= 0 {
		capacity = 100
	}
	buffer := &ringBufferInt{
		capacity: capacity,
	}

	var previousRC *ringCell
	for i := range capacity {
		rc := &ringCell{}
		if i == 0 {
			buffer.read = rc
		}
		if previousRC != nil {
			previousRC.next = rc
		}
		previousRC = rc
	}

	previousRC.next = buffer.read
	buffer.write = buffer.read

	return buffer
}

func (r *ringBufferInt) Add(item int) {
	r.write.value = item
	r.write = r.write.next
	r.currentCount += 1
}

func (r *ringBufferInt) Release() (item int) {
	item = r.read.value

	r.read = r.read.next

	r.currentCount -= 1

	return item
}

func (r *ringBufferInt) GetCount() int {
	return r.currentCount
}
