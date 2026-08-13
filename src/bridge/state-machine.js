// src/bridge/state-machine.js
// Pure-function state machine for the realtime voice bridge (Phase 1).
// Zero side effects: no IO, no global state, no logging — safe for unit testing.
// Voice loop: IDLE -> LISTENING -> RECOGNIZING -> THINKING -> SPEAKING,
// with BARGE_IN returning any active state to LISTENING (barge-in loop).

// All valid bridge states.
export const STATES = Object.freeze({
  IDLE: "IDLE",
  LISTENING: "LISTENING",
  RECOGNIZING: "RECOGNIZING",
  THINKING: "THINKING",
  SPEAKING: "SPEAKING",
});

// All events that can drive a state transition.
export const EVENTS = Object.freeze({
  INPUT_START: "INPUT_START",
  SPEECH_END: "SPEECH_END",
  RECOGNIZED: "RECOGNIZED",
  TTS_START: "TTS_START",
  TTS_END: "TTS_END",
  TEXT_INPUT: "TEXT_INPUT",
  BARGE_IN: "BARGE_IN",
  RESET: "RESET",
  CLOSE: "CLOSE",
});

// Legal (state, event) -> nextState table.
// RESET/CLOSE are listed on every state (including IDLE) so teardown is idempotent.
// LISTENING + BARGE_IN is idempotent: already listening, so barge-in is a no-op.
export const TRANSITIONS = Object.freeze({
  [STATES.IDLE]: Object.freeze({
    [EVENTS.INPUT_START]: STATES.LISTENING,
    [EVENTS.TEXT_INPUT]: STATES.THINKING,
    [EVENTS.RESET]: STATES.IDLE,
    [EVENTS.CLOSE]: STATES.IDLE,
  }),
  [STATES.LISTENING]: Object.freeze({
    [EVENTS.SPEECH_END]: STATES.RECOGNIZING,
    [EVENTS.BARGE_IN]: STATES.LISTENING,
    [EVENTS.RESET]: STATES.IDLE,
    [EVENTS.CLOSE]: STATES.IDLE,
  }),
  [STATES.RECOGNIZING]: Object.freeze({
    [EVENTS.RECOGNIZED]: STATES.THINKING,
    [EVENTS.BARGE_IN]: STATES.LISTENING,
    [EVENTS.RESET]: STATES.IDLE,
    [EVENTS.CLOSE]: STATES.IDLE,
  }),
  [STATES.THINKING]: Object.freeze({
    [EVENTS.BARGE_IN]: STATES.LISTENING,
    [EVENTS.TTS_START]: STATES.SPEAKING,
    [EVENTS.RESET]: STATES.IDLE,
    [EVENTS.CLOSE]: STATES.IDLE,
  }),
  [STATES.SPEAKING]: Object.freeze({
    [EVENTS.BARGE_IN]: STATES.LISTENING,
    [EVENTS.TTS_END]: STATES.IDLE,
    [EVENTS.RESET]: STATES.IDLE,
    [EVENTS.CLOSE]: STATES.IDLE,
  }),
});

const hasOwn = (obj, key) => Object.prototype.hasOwnProperty.call(obj, key);

// Returns true when (state, event) is a legal transition. Never throws (guard use).
export function canTransition(state, event) {
  return (
    hasOwn(TRANSITIONS, state) &&
    hasOwn(EVENTS, event) &&
    hasOwn(TRANSITIONS[state], event)
  );
}

// Returns the next state for a legal (state, event); throws on unknown/illegal input.
export function transition(state, event) {
  if (!hasOwn(TRANSITIONS, state)) {
    throw new Error(`Unknown state: ${state}`);
  }
  if (!hasOwn(EVENTS, event)) {
    throw new Error(`Unknown event: ${event}`);
  }
  if (!hasOwn(TRANSITIONS[state], event)) {
    throw new Error(`Invalid transition: ${state} + ${event}`);
  }
  return TRANSITIONS[state][event];
}
