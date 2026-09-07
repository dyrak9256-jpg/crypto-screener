package mexc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func appendVarint(dst []byte, value uint64) []byte {
	for value >= 0x80 {
		dst = append(dst, byte(value)|0x80)
		value >>= 7
	}
	return append(dst, byte(value))
}

func appendBytes(dst []byte, field int, value []byte) []byte {
	dst = appendVarint(dst, uint64(field<<3|2))
	dst = appendVarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func appendString(dst []byte, field int, value string) []byte {
	return appendBytes(dst, field, []byte(value))
}

func appendInt64(dst []byte, field int, value int64) []byte {
	dst = appendVarint(dst, uint64(field<<3))
	return appendVarint(dst, uint64(value))
}

func TestDecodeMEXCSpotKline(t *testing.T) {
	kline := []byte{}
	kline = appendString(kline, 1, "Min1")
	kline = appendInt64(kline, 2, 1736410500)
	kline = appendString(kline, 3, "92925")
	kline = appendString(kline, 4, "93158.47")
	kline = appendString(kline, 7, "36.83803224")
	kline = appendString(kline, 8, "3424811.05")
	kline = appendInt64(kline, 9, 1736410559)

	wrapper := []byte{}
	wrapper = appendString(wrapper, 1, "spot@public.kline.v3.api.pb@BTCUSDT@Min1")
	wrapper = appendString(wrapper, 3, "BTCUSDT")
	wrapper = appendInt64(wrapper, 5, 1736410500000)
	wrapper = appendInt64(wrapper, 6, 1736410500123)
	wrapper = appendBytes(wrapper, 308, kline)

	got, err := decodeMEXCSpotKline(wrapper)
	require.NoError(t, err)
	require.Equal(t, "BTCUSDT", got.Symbol)
	require.Equal(t, "Min1", got.Interval)
	require.Equal(t, int64(1736410500), got.WindowStart)
	require.Equal(t, int64(1736410559), got.WindowEnd)
	require.Equal(t, "3424811.05", got.Amount)
	require.Equal(t, int64(1736410500123), got.SendTime)
}

func TestDecodeMEXCSpotKlineRejectsMissingPayload(t *testing.T) {
	_, err := decodeMEXCSpotKline(appendString(nil, 1, "spot@public.kline.v3.api.pb@BTCUSDT@Min1"))
	require.Error(t, err)
}

// FuzzDecodeMEXCSpotKline гарантирует, что ручной protobuf-парсер не паникует
// на произвольных байтах: MEXC-канал данных не контролируется нами.
func FuzzDecodeMEXCSpotKline(f *testing.F) {
	kline := []byte{}
	kline = appendString(kline, 1, "Min1")
	kline = appendInt64(kline, 2, 1736410500)
	kline = appendInt64(kline, 3, 1736410500)
	f.Add(kline)
	f.Add([]byte{})
	f.Add([]byte{0xff})
	f.Add([]byte{0x08, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01})
	f.Add([]byte{0x1a, 0x05, 'h', 'e', 'l', 'l', 'o'})
	f.Fuzz(func(t *testing.T, data []byte) {
		// Ошибка допустима (мусорный ввод), паника или зависание — нет.
		out, err := decodeMEXCSpotKline(data)
		if err == nil {
			_ = out
		}
	})
}
