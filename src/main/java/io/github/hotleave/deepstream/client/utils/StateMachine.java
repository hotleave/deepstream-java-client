package io.github.hotleave.deepstream.client.utils;

import io.github.hotleave.deepstream.client.connection.ConnectionState;
import lombok.*;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

@FieldDefaults(level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
@Slf4j
public class StateMachine {
    final List<Transition> transitions;
    final BiConsumer<ConnectionState, ConnectionState> stateChangeListener;
    final List<Map<String, Object>> history = new ArrayList<>();
    @Getter
    ConnectionState state = ConnectionState.CLOSED;

    public void transition(String name) {
        for (Transition transition : transitions) {
            if (Objects.equals(transition.name, name) && (state == transition.from || transition.from == null)) {
                history.add(Map.ofEntries(
                        Map.entry("oldState", state),
                        Map.entry("newState", transition.to),
                        Map.entry("transitionName", name)
                ));
                var oldState = state;
                state = transition.to;

                if (stateChangeListener != null) {
                    stateChangeListener.accept(state, oldState);
                }

                if (transition.handler != null) {
                    transition.handler.run();
                }

                return;
            }
        }

        // From initial state to current state
        log.error("Invalid state transition: {}, [state={}, transitionName={}]", history, state, name);
    }

    @Data
    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
    @AllArgsConstructor
    public static class Transition {
        String name;
        ConnectionState from;
        ConnectionState to;
        Runnable handler;

        public Transition(String name, ConnectionState from, ConnectionState to) {
            this.name = name;
            this.from = from;
            this.to = to;
            handler = null;
        }
    }
}
