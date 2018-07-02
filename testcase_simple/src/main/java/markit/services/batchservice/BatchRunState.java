package markit.services.batchservice;

/**
 * States of BatchRun. Initialization for possible multithreading version.
 * @author aleksandr tavgen
 */

public enum BatchRunState {
    INITIALISATION,
    STARTED,
    UPLOAD,
    COMPLETED,
    CANCELED
}
