package log

type IMessage interface {
    String() string
    check(log *Log, logEntry *LogEntry) error
    handle(log *Log, logEntry *LogEntry) error
    asyncHandle(log *Log, logEntry *LogEntry) error
}
