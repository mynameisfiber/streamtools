package blocks
// Current implemintation if from: http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
// Future: implement this instead -- http://static.googleusercontent.com/external_content/untrusted_dlcp/research.google.com/en/us/pubs/archive/40671.pdf

import (
	"errors"
	"github.com/reusee/mmh3"
	"log"
	"math"
)

var (
	// Various errors that can be thrown
	InvalidRuleB = errors.New("Invalid value of `b`.  4 <= b <= 16")
)

var (
    // constants
    _2Raised32 = float64(1 << 32)
    _2Raised32Over30 = float64(1 << 32) / 30.0
    _5Halfs = float64(5.0 / 2.0)
)

// the rule defines how the block works. It can be get and set live
type hllRule struct {
	B        uint8
	Key      string
}

// the state defines the current state of the block. This is where we store
// whatever is being learned about the data
type hllState struct {
	M     []uint8
	Alpha float64
	rule  *hllRule
}

func (h *hllState) Add(key, value string) {
	hash := mmh3.Hash32([]byte(value))
	j, w := SplitInt(hash, h.rule.B)
	rho := LeftmostSetBit(w) - uint8(h.rule.B)
    log.Printf("hash: %b\n", hash)
    log.Printf("j:    %b\n", j)
    log.Printf("w:    %b\n", w)
    log.Printf("rho:  %d\n\n", rho)
	if h.M[j] < rho {
		h.M[j] = rho
	}
}

func (h *hllState) Cardinality() float64 {
    m := float64(len(h.M))
    ETop := h.Alpha * m * m
    EBottom := 0.0
    V := 0.0
    for _, value := range h.M {
        EBottom += math.Pow(2, -1.0 * float64(value))
        if value == 0 {
            V += 1.0
        }
    }
    E := ETop / EBottom
    
    switch {
        case E <= _5Halfs * m:
            if V == 0.0 {
                return E
            }
            return m * math.Log2(m / V)
        case E <= _2Raised32Over30:
            return E
        default:
            return -1.0 * _2Raised32 * math.Log2(1 - E / _2Raised32)
    }
}

func LeftmostSetBit(n uint32) uint8 {
	// Returns the index of the leftmost set bit
	return 32 - uint8(math.Log2(float64(n))+1)
}

func SplitInt(N uint32, index uint8) (leading, trailing uint32) {
	offset := uint(32 - index)
	leading = N >> offset
	trailing = N ^ (leading << offset)
	return leading, trailing
}

func NewHLLState(rule *hllRule) (*hllState, error) {
	if rule.B < 4 || rule.B > 16 {
		return nil, InvalidRuleB
	}
	m := uint(math.Pow(2, float64(rule.B)))
	M := make([]uint8, m)
	var alpha float64
	switch m {
	case 16:
		alpha = 0.673
	case 32:
		alpha = 0.697
	case 64:
		alpha = 0.709
	default:
		alpha = 0.7213 / (1.0 + 1.079/float64(m))
	}
	state := hllState{M: M, Alpha: alpha, rule: rule}
	return &state, nil
}

// this is a skeleton state block. It doesn't do anything, but can be used as a
// template to make new state blocks.
func HLL(b *Block) {
	// we initialise the data and the rule. It's often helpful to provide an
	// initial state of the block
	data := &hllState{}
	var rule *hllRule

	for {
		select {
		case query := <-b.Routes["cardinality"]:
			// the state route is used to retrieve the state of the block
            cardinality := data.Cardinality()
            log.Println("Found cardinality: ", cardinality)
            marshal(query, map[string]float64{"cardinality":cardinality})
		case ruleUpdate := <-b.Routes["set_rule"]:
			// the set rule route is used to update the block's rule. There are two
			// situations: if we've not seen any rules before - i.e. this is a brand
			// new block, or if this is a block that already had rules.
            newRule := &hllRule{}
			unmarshal(ruleUpdate, newRule)
            if newRule == nil {
                log.Println("HLL: Could not parse new rule")
                continue
            }
            if rule == nil || newRule.B != rule.B {
				var err error
				data, err = NewHLLState(newRule)
				if err != nil {
					// if a bad rule set gets sent in, log to STDOUT and un-initialize this block
                    log.Println("HLL: Error setting state for new rule: ", err.Error())
					data = &hllState{}
					rule = nil
					continue
				}
			}
			rule = newRule
		case msg := <-b.Routes["get_rule"]:
			// the get rule route reports the current rule being used by the block
			if rule == nil {
				marshal(msg, &hllRule{})
			} else {
				marshal(msg, rule)
			}
		case <-b.QuitChan:
			// the quit chan recieves a signal that tells this block to
			// terminate. Most of the time all you need do is pass the block to
			// the quit function
			quit(b)
			return
        case msg := <-b.InChan:
			// the in chan recieves in bound messages. This is usually where the
			// bulk of the processing goes. In a state block, this is where the
			// state should be updated
			if rule == nil {
                log.Println("exiting HLL add due to empty rule")
				break
			}

			valueSlice := getKeyValues(msg.Msg, rule.Key)
			// we need to guard against the possibility that the key is not in
			// the message
			if len(valueSlice) == 0 {
                log.Println("HLL: could not find", rule.Key, "in message: ", msg)
				break
			}

			value := valueSlice[0]
			valueString, ok := value.(string)
			if !ok {
                log.Println("HLL: nil value against", rule.Key, " - ignoring")
				break
			}
            data.Add("", valueString)
		}
	}
}
