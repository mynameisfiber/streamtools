package blocks

import (
	"github.com/mynameisfiber/gohll"
	"log"
    "fmt"
)

// the state defines the current state of the block. This is where we store
// whatever is being learned about the data
type hllState struct {
    Hlls map[string]*gohll.HLL
    rule *hllRule
}

func NewHLLState(rule *hllRule) *hllState {
    return &hllState{
        Hlls: make(map[string]*gohll.HLL),
        rule: rule,
    }
}

// the rule defines how the block works. It can be get and set live
type hllRule struct {
	ErrorRate float64
	Key      string
	Value    string
}


func (h *hllState) Add(key, value string) {
    fmt.Println("Adding value: ", value)
    if hll, ok := h.Hlls[key]; ok {
        hll.Add(value)
    } else {
        h.Hlls[key], _ = gohll.NewHLLByError(h.rule.ErrorRate)
        h.Hlls[key].Add(value)
    }
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
            result := make(map[string]float64)
            for key, hll := range data.Hlls {
                result[key] = hll.Cardinality()
            }
            marshal(query, result)
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
            if rule == nil || newRule.ErrorRate != rule.ErrorRate {
				var err error
				data = NewHLLState(newRule)
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

			valueSlice := getKeyValues(msg.Msg, rule.Value)
			// we need to guard against the possibility that the key is not in
			// the message
			if len(valueSlice) == 0 {
                log.Println("HLL: could not find", rule.Value, "in message: ", msg)
				break
			}

			value := valueSlice[0]
			valueString, ok := value.(string)
			if !ok {
                log.Println("HLL: nil value against", rule.Value, " - ignoring")
				break
			}
            data.Add("", valueString)
		}
	}
}
