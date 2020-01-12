//package com.jarcadia.watchdog;
//
//import java.util.concurrent.atomic.AtomicBoolean;
//
//import com.jarcadia.rcommando.RedisObject;
//
//public final class AgentTaskCompletable {
//
//    private final AtomicBoolean completed;
//    private final RedisObject obj;
//    private final String field;
//    private final Object value;
//
//    protected AgentTaskCompletable(RedisObject obj, String field, Object value) {
//        this.completed = new AtomicBoolean(false);
//        this.obj = obj;
//        this.field = field;
//        this.value = value;
//    }
//
//    public void complete() {
//        if (completed.compareAndSet(false, true)) {
//            obj.checkedSet(field, value);
//        } else {
//            throw new RuntimeException("TaskCompleteable has already been completed");
//        }
//    }
//}
