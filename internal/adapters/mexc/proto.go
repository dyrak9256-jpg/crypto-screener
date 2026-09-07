package mexc

import (
	"fmt"
)

// mexcSpotKline is the subset of the MEXC PushDataV3ApiWrapper protobuf
// message required by the screener. The official wrapper puts PublicSpotKline
// in oneof field 308 and the kline payload uses fields 1..9.
type mexcSpotKline struct {
	Channel     string
	Symbol      string
	CreateTime  int64
	SendTime    int64
	Interval    string
	WindowStart int64
	Amount      string
	WindowEnd   int64
}

func decodeMEXCSpotKline(data []byte) (mexcSpotKline, error) {
	var out mexcSpotKline
	var klinePayload []byte
	for len(data) > 0 {
		field, wire, n, err := readProtoKey(data)
		if err != nil {
			return mexcSpotKline{}, fmt.Errorf("read wrapper key: %w", err)
		}
		data = data[n:]
		switch wire {
		case 0:
			value, n, err := readProtoVarint(data)
			if err != nil {
				return mexcSpotKline{}, fmt.Errorf("read wrapper field %d: %w", field, err)
			}
			data = data[n:]
			switch field {
			case 5:
				out.CreateTime = int64(value)
			case 6:
				out.SendTime = int64(value)
			}
		case 2:
			switch field {
			case 1:
				value, n, err := readProtoBytes(data)
				if err != nil {
					return mexcSpotKline{}, fmt.Errorf("read wrapper channel: %w", err)
				}
				data = data[n:]
				out.Channel = string(value)
			case 3, 4:
				value, n, err := readProtoBytes(data)
				if err != nil {
					return mexcSpotKline{}, fmt.Errorf("read wrapper field %d: %w", field, err)
				}
				data = data[n:]
				if field == 3 {
					out.Symbol = string(value)
				}
			case 308:
				value, n, err := readProtoBytes(data)
				if err != nil {
					return mexcSpotKline{}, fmt.Errorf("read wrapper spot kline: %w", err)
				}
				data = data[n:]
				klinePayload = append(klinePayload[:0], value...)
			default:
				// Unknown/non-used field. Skip it safely.
				data, err = skipProtoBytes(data)
				if err != nil {
					return mexcSpotKline{}, fmt.Errorf("skip wrapper field %d: %w", field, err)
				}
			}
		case 1:
			if len(data) < 8 {
				return mexcSpotKline{}, fmt.Errorf("skip wrapper field %d: truncated fixed64", field)
			}
			data = data[8:]
		case 5:
			if len(data) < 4 {
				return mexcSpotKline{}, fmt.Errorf("skip wrapper field %d: truncated fixed32", field)
			}
			data = data[4:]
		default:
			return mexcSpotKline{}, fmt.Errorf("skip wrapper field %d: unsupported wire type %d", field, wire)
		}
	}
	if len(klinePayload) == 0 {
		return mexcSpotKline{}, fmt.Errorf("wrapper does not contain publicSpotKline field 308")
	}
	if err := decodeMEXCKlinePayload(klinePayload, &out); err != nil {
		return mexcSpotKline{}, fmt.Errorf("decode publicSpotKline: %w", err)
	}
	return out, nil
}

func decodeMEXCKlinePayload(data []byte, out *mexcSpotKline) error {
	for len(data) > 0 {
		field, wire, n, err := readProtoKey(data)
		if err != nil {
			return fmt.Errorf("read key: %w", err)
		}
		data = data[n:]
		if wire == 0 {
			value, n, err := readProtoVarint(data)
			if err != nil {
				return fmt.Errorf("read varint field %d: %w", field, err)
			}
			data = data[n:]
			switch field {
			case 2:
				out.WindowStart = int64(value)
			case 9:
				out.WindowEnd = int64(value)
			}
			continue
		}
		if wire != 2 {
			return fmt.Errorf("unsupported wire type %d for field %d", wire, field)
		}
		value, n, err := readProtoBytes(data)
		if err != nil {
			return fmt.Errorf("read bytes field %d: %w", field, err)
		}
		data = data[n:]
		switch field {
		case 1:
			out.Interval = string(value)
		case 8:
			out.Amount = string(value)
		}
	}
	return nil
}

func readProtoKey(data []byte) (field int, wire int, n int, err error) {
	value, n, err := readProtoVarint(data)
	if err != nil {
		return 0, 0, 0, err
	}
	field = int(value >> 3)
	wire = int(value & 7)
	if field <= 0 {
		return 0, 0, 0, fmt.Errorf("invalid field number %d", field)
	}
	return field, wire, n, nil
}

func readProtoVarint(data []byte) (uint64, int, error) {
	var value uint64
	for i, b := range data {
		if i >= 10 {
			return 0, 0, fmt.Errorf("varint exceeds 10 bytes")
		}
		value |= uint64(b&0x7f) << (7 * i)
		if b < 0x80 {
			return value, i + 1, nil
		}
	}
	return 0, 0, fmt.Errorf("truncated varint")
}

func readProtoBytes(data []byte) ([]byte, int, error) {
	length, n, err := readProtoVarint(data)
	if err != nil {
		return nil, 0, err
	}
	if length > uint64(len(data)-n) {
		return nil, 0, fmt.Errorf("length %d exceeds remaining payload %d", length, len(data)-n)
	}
	end := n + int(length)
	return data[n:end], end, nil
}

func skipProtoBytes(data []byte) ([]byte, error) {
	_, n, err := readProtoBytes(data)
	if err != nil {
		return nil, err
	}
	return data[n:], nil
}
