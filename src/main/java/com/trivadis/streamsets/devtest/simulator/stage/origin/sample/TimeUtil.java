package com.trivadis.streamsets.devtest.simulator.stage.origin.sample;

public class TimeUtil {
    /**
     * Calculates the current soccer match timestamp (in ms) from the timestamp (in ms) the time provider returns.
     *
     * @param currentMachineTimestampMs              Current machine timestamp (in ms) returned by the time provider
     * @param machineTimestampWhenStartingSimulationMs Machine timestamp (in ms) returned by the time provider when simulation starts
     * @param simulationStartTimestampMs              timestamp (in ms) when event to be simulated started
     * @param simulationSpeedup                    Speedup multiplier for the simulation
     * @return Current simulation event timestamp (in ms)
     */
    public static long generateTimestamp (long currentMachineTimestampMs
                                            , long machineTimestampWhenStartingSimulationMs
                                            , long simulationStartTimestampMs
                                            , double simulationSpeedup) {
        long machineDiffMs = currentMachineTimestampMs - machineTimestampWhenStartingSimulationMs;

        return (long) (simulationStartTimestampMs + machineDiffMs * simulationSpeedup);
    }
}
