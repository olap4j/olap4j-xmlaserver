package mondrian.xmla;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.olap4j.OlapStatement;
import org.olap4j.impl.Olap4jUtil;

public class XmlaSessionManager {
    // Default session time out is 2 hours.
    public static final long DEFAULT_SESSION_TIME_OUT = 2 * 60 * 60 * 1000;
    private static XmlaSessionManager instance;
    /*
     * A map of all current sessions by keyed by their sessionId.
     */
    private final Map<String, XmlaSession> sessionMap = new HashMap<String, XmlaSession>();
    private final Timer timer;
    private final long sessionTimeOut;
    
    private XmlaSessionManager(long sessionTimeOut) {
        this.sessionTimeOut = sessionTimeOut;
        timer = new Timer("SessionTimer", true);
        // Check for session time out four times more often than they time out
        long period = sessionTimeOut / 4;
        timer.schedule(
            new TimerTask() {
                public void run() {
                    timeoutSessions();
                }
            },
            period, period);
    }
    public static XmlaSessionManager getInstance() {
        if (instance == null) {
            instance = new XmlaSessionManager(DEFAULT_SESSION_TIME_OUT);
        }
        return instance;
    }
    public static XmlaSessionManager createInstance(long sessionTimeOut) {
        instance = new XmlaSessionManager(sessionTimeOut);
        return instance;
    }

    private XmlaSession getOrCreateSession(String sessionId) {
        XmlaSession session;
        synchronized (sessionMap) {
            session = sessionMap.get(sessionId);
            if (session == null) {
                session = new XmlaSession();
                sessionMap.put(sessionId, session);
            }
        }
        return session;
    }

    public void registerSessionWithCredentials(
        String sessionId, String username, String password)
    {
        synchronized (sessionMap) {
            XmlaSession session = sessionMap.get(sessionId);
            if (session != null
                && Olap4jUtil.equal(session.getUsername(), username))
            {
                // Overwrite the password, but only if it is non-empty.
                // (Sometimes Simba sends the credentials object again
                // but without a password.)
                if (password != null && password.length() > 0) {
                    session.setPassword(password);
                }
            } else {
                // A credentials object was stored against the provided session
                // ID but the username didn't match, so create a new holder.
                session = new XmlaSession();
                session.setUsername(username);
                session.setPassword(password);
                sessionMap.put(sessionId, session);
            }
        }
    }
    public String[] getSessionCredentials(String sessionId) {
        XmlaSession session;
        synchronized (sessionMap) {
            session = sessionMap.get(sessionId);
        }
        if (session == null) {
            return null;
        } else {
            String[] credentials = new String[2];
            credentials[0] = session.getUsername();
            credentials[1] = session.getPassword();
            return credentials;
        }
    }
    
    public void cancelSession(String sessionId) {
        XmlaSession session = getOrCreateSession(sessionId);
        session.cancel();
        // Do not remove a cancelled session so that we can cancel any new statements.
    }
    public void addStatementToSession(String sessionId, OlapStatement statement, String command) {
        XmlaSession session = getOrCreateSession(sessionId);
        session.addStatement(statement, command);
    }
    public void removeStatementFromSession(String sessionId, OlapStatement statement) {
        XmlaSession session;
        synchronized (sessionMap) {
            session = sessionMap.get(sessionId);
        }
        if (session != null) {
            session.removeStatement(statement);
        }
    }
    public void endSession(String sessionId) {
        synchronized (sessionMap) {
            sessionMap.remove(sessionId);
        }
    }
    public void timeoutSessions() {
        long now = System.currentTimeMillis();
        synchronized (sessionMap) {
            for (Iterator<XmlaSession> iter = sessionMap.values().iterator(); iter.hasNext();) {
                XmlaSession session = iter.next();
                if (now - session.lastModified > sessionTimeOut) {
                    iter.remove();
                }
            }
        }
    }

    public Map<String, XmlaSession> getAllSessions() {
        Map<String, XmlaSession> copiedSessionMap;;
        synchronized (sessionMap) {
            copiedSessionMap = new HashMap<String, XmlaSession>(sessionMap);
        }
        return copiedSessionMap;
    }

    public static class XmlaSession {
        private String username;
        private String password;
        private Map<OlapStatement, XmlaSessionCommand> statements;
        private String lastCommand;
        private boolean isCancelled;
        private long started;
        private long lastCommandStarted;
        private long lastCommandFinished;
        private long lastModified;
        private int totalCommands;

        public XmlaSession() {
            statements = new HashMap<OlapStatement, XmlaSessionCommand>();
            isCancelled = false;
            started = System.currentTimeMillis();
            totalCommands = 0;
        }
        public void cancel() {
            isCancelled = true;
            synchronized (statements) {
                for (XmlaSessionCommand command : statements.values()) {
                    try {
                        command.getStatement().cancel();
                    } catch (SQLException e) {
                        // ignore
                    }
                }
                statements.clear();
                lastModified = System.currentTimeMillis();
            }
        }
        public void addStatement(OlapStatement statement, String command) {
            if (isCancelled) {
                try {
                    statement.cancel();
                } catch (SQLException e) {
                    // ignore
                }
            } else {
                synchronized (statements) {
                    lastModified = System.currentTimeMillis();
                    statements.put(statement, new XmlaSessionCommand(statement, lastModified, command));
                    lastCommandStarted = lastModified;
                    totalCommands++;
                    lastCommand = command;
                }
            }
        }
        public void removeStatement(OlapStatement statement) {
            synchronized (statements) {
                statements.remove(statement);
                lastModified = System.currentTimeMillis();
                lastCommandFinished = lastModified;
            }
        }
        public Collection<XmlaSessionCommand> getAllCommands() {
            synchronized (statements) {
                return statements.values();
            }
        }
        public String getUsername() {
            return username;
        }
        public void setUsername(String username) {
            synchronized (statements) {
                this.username = username;
                lastModified = System.currentTimeMillis();
            }
        }
        public String getPassword() {
            return password;
        }
        public void setPassword(String password) {
            synchronized (statements) {
                this.password = password;
                lastModified = System.currentTimeMillis();
            }
        }
        public boolean isCancelled() {
            return isCancelled;
        }
        public long getStarted() {
            return started;
        }
        public long getLastCommandStarted() {
            return lastCommandStarted;
        }
        public long getLastCommandFinished() {
            return lastCommandFinished;
        }
        public int getTotalCommandCount() {
            return totalCommands;
        }
        public int getCurrentCommandCount() {
            synchronized (statements) {
                return statements.size();
            }
        }
        public String getLastCommand() {
            return lastCommand;
        }
    }

    public static class XmlaSessionCommand {
        private OlapStatement statement;
        private long startTime;
        private String command;

        public XmlaSessionCommand(OlapStatement statement, long startTime, String command) {
            this.statement = statement;
            this.startTime = startTime;
            this.command = command;
        }
        public String getCommand() {
            return command;
        }
        public long getStartTime() {
            return startTime;
        }
        public OlapStatement getStatement() {
            return statement;
        }
    }
}
