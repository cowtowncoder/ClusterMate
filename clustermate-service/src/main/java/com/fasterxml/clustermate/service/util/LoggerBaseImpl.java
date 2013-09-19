package com.fasterxml.clustermate.service.util;

import org.slf4j.helpers.MarkerIgnoringBase;
import org.slf4j.helpers.MessageFormatter;

/**
 * Helper class added to make it easier to use alternate logging
 * systems, if slf4j won't work. Implementation leaves out support
 * for debug, trace (who needs them anyway?); assumes 'warn' and 'error'
 * are enabled by default.
 *<p>
 * NOTE: NOT COMPLETE -- NOT YET READY FOR USE!
 */
@SuppressWarnings("serial")
public abstract class LoggerBaseImpl
	extends MarkerIgnoringBase
{
    /*
    /**********************************************************************
    /* Abstract methods for impls
    /**********************************************************************
     */

	protected abstract void _debug(String message);
	protected abstract void _info(String message);
	protected abstract void _warn(String message);
	protected abstract void _error(String message);

	/*
    /**********************************************************************
    /* Level check methods
    /**********************************************************************
     */

	// Trace, Debug disabled
	@Override
	public boolean isTraceEnabled() {
		return false;
	}

	@Override
	public boolean isDebugEnabled() {
		return false;
	}
	
	@Override
	public abstract boolean isInfoEnabled();

	// By default we log warnings
	@Override
	public boolean isWarnEnabled() {
		return true;
	}

	// By default, yes, we do log errors
	@Override
	public boolean isErrorEnabled() {
		return true;
	}
	
    /*
    /**********************************************************************
    /* We never use trace, debug, add bogus impls
    /**********************************************************************
     */

	@Override
	public void trace(String msg) { }

	@Override
	public void trace(String format, Object arg) { }

	@Override
	public void trace(String format, Object arg1, Object arg2) { }

	@Override
	public void trace(String format, Object... arguments) { }

	@Override
	public void trace(String msg, Throwable t) { }

	@Override
	public void debug(String msg) { }

	@Override
	public void debug(String format, Object arg) { }

	@Override
	public void debug(String format, Object arg1, Object arg2) { }

	@Override
	public void debug(String format, Object... arguments) { }

	@Override
	public void debug(String msg, Throwable t) { }

    /*
    /**********************************************************************
    /* Info, Warn, Error are supported
    /**********************************************************************
     */

	@Override
	public void info(String msg) {
		if (isInfoEnabled()) {
			_info(msg);
		}
	}

	@Override
	public void info(String format, Object arg) {
		if (isInfoEnabled()) {
			_info(_format(format, arg));
		}
	}

	@Override
	public void info(String format, Object arg1, Object arg2) {
		if (isInfoEnabled()) {
			_info(_format(format, arg1, arg2));
		}
	}

	@Override
	public void info(String format, Object... arguments) {
		if (isInfoEnabled()) {
			_info(_format(format, arguments));
		}
	}

	@Override
	public void info(String msg, Throwable t) {
		if (isInfoEnabled()) {
			_info(_format(msg, t));
		}
	}

	@Override
	public void warn(String msg) {
		if (isWarnEnabled()) {
			_warn(_format(msg));
		}
	}

	@Override
	public void warn(String format, Object arg) {
		if (isWarnEnabled()) {
			_warn(_format(format, arg));
		}
	}

	@Override
	public void warn(String format, Object... arguments) {
		if (isWarnEnabled()) {
			_warn(_format(format, arguments));
		}
	}

	@Override
	public void warn(String format, Object arg1, Object arg2) {
		if (isWarnEnabled()) {
			_warn(_format(format, arg1, arg2));
		}
	}

	@Override
	public void warn(String msg, Throwable t) {
		if (isWarnEnabled()) {
			_warn(_format(msg, t));
		}
	}

	@Override
	public void error(String msg) {
		if (isErrorEnabled()) {
			_error(msg);
		}
	}

	@Override
	public void error(String format, Object arg) {
		if (isErrorEnabled()) {
			_error(_format(format, arg));
		}
	}

	@Override
	public void error(String format, Object arg1, Object arg2) {
		if (isErrorEnabled()) {
			_error(_format(format, arg1, arg2));
		}
	}

	@Override
	public void error(String format, Object... arguments) {
		if (isErrorEnabled()) {
			error(_format(format, arguments));
		}
	}

	@Override
	public void error(String msg, Throwable t) {
		if (isErrorEnabled()) {
			error(_format(msg, t));
		}
	}

    /*
    /**********************************************************************
    /* Helper methods needed for actual formatting, with default impls
    /**********************************************************************
     */

	protected String _format(String format, Object arg) {
		return MessageFormatter.format(format, arg).toString();
	}

	protected String _format(String format, Object arg1, Object arg2) {
		return MessageFormatter.format(format, arg1, arg2).toString();
	}

	protected String _format(String format, Object... args) {
		return MessageFormatter.format(format, args).toString();
	}

	protected String _format(String msg, Throwable t) {
		if (t == null) {
			return msg;
		}
		String etype = t.getClass().getName();
		StringBuilder sb = new StringBuilder(msg.length() + etype.length() + 3);
		sb.append(msg);
		sb.append(" (").append(t.getClass().getName()).append(")");
		return sb.toString();
	}
}
