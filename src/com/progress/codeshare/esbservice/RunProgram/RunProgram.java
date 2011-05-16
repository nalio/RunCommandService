package com.progress.codeshare.esbservice.RunProgram;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import com.sonicsw.xq.XQConstants;
import com.sonicsw.xq.XQEnvelope;
import com.sonicsw.xq.XQInitContext;
import com.sonicsw.xq.XQLog;
import com.sonicsw.xq.XQMessage;
import com.sonicsw.xq.XQMessageException;
import com.sonicsw.xq.XQParameterInfo;
import com.sonicsw.xq.XQParameters;
import com.sonicsw.xq.XQPart;
import com.sonicsw.xq.XQProcessContext;
import com.sonicsw.xq.XQServiceContext;
import com.sonicsw.xq.XQServiceEx;
import com.sonicsw.xq.XQServiceException;

public class RunProgram implements XQServiceEx {

	// This is the XQLog (the container's logging mechanism).
	private XQLog m_xqLog = null;

	// This is the the log prefix that helps identify this service during
	// logging
	private String m_logPrefix = "";

	// These hold version information.
	private static int s_major = 4;

	private static int s_minor = 0;

	private static int s_buildNumber = 0;

	private String m_defaultProgramParameters = null;

	private String m_programImagePath = null;

	private ProcessBuilder procBuilder = null;

	private Process proc = null;

	/**
	 * Constructor for a RunProgram
	 */
	public RunProgram() {
	}

	/**
	 * Initialize the XQService by processing its initialization parameters.
	 * 
	 * <p>
	 * This method implements a required XQService method.
	 * 
	 * @param initialContext
	 *            The Initial Service Context provides access to:<br>
	 *            <ul>
	 *            <li>The configuration parameters for this instance of the
	 *            RunProgram.</li>
	 *            <li>The XQLog for this instance of the RunProgram.</li>
	 *            </ul>
	 * @exception XQServiceException
	 *                Used in the event of some error.
	 */
	public void init(XQInitContext initialContext) throws XQServiceException {
		XQParameters params = initialContext.getParameters();
		m_xqLog = initialContext.getLog();
		setLogPrefix(params);
		m_xqLog.logInformation(m_logPrefix + " Initializing ...");

		writeStartupMessage(params);
		writeParameters(params);
		// perform initilization work.
		m_programImagePath = params.getParameter("ProgramImagePath",
				XQConstants.PARAM_STRING);
		m_defaultProgramParameters = params.getParameter("ProgramParameters",
				XQConstants.PARAM_STRING);

		StringTokenizer paramTokens = new StringTokenizer(
				m_defaultProgramParameters);
		int numParams = paramTokens.countTokens();
		String[] cmdArray = new String[numParams + 1];
		cmdArray[0] = m_programImagePath;
		for (int p = 1; p <= numParams; p++) {
			cmdArray[p] = paramTokens.nextToken();
		}
		procBuilder = new ProcessBuilder(cmdArray);
		procBuilder.redirectErrorStream(true);
		m_xqLog.logInformation(m_logPrefix + " Initialized ...");
	}

	/**
	 * Handle the arrival of XQMessages in the INBOX.
	 * 
	 * <p>
	 * This method implement a required XQService method.
	 * 
	 * @param ctx
	 *            The service context.
	 * @exception XQServiceException
	 *                Thrown in the event of a processing error.
	 */
	@SuppressWarnings("static-access")
	public void service(XQServiceContext ctx) throws XQServiceException {
		m_xqLog.logDebug(m_logPrefix + "Service processing...");

		// Get the message.
		XQEnvelope env = null;
		while (ctx.hasNextIncoming()) {
			env = ctx.getNextIncoming();
			if (env != null) {
				XQMessage msg = env.getMessage();
				try {
					XQParameters params = ctx.getParameters();
					int iPartCnt = 0;
					try {
					    iPartCnt = Integer.parseInt(params.getParameter("MessagePart",
							XQConstants.PARAM_STRING));
					} catch (Exception e) {
						m_xqLog.logWarning(m_logPrefix + "MessagePart parameter has invalid integer: " + params.getParameter("MessagePart",
							XQConstants.PARAM_STRING) + ". Taking default instead: 0.");
					}
					// XQPart prt = msg.getPart(i);
					XQPart prt = msg.getPart(iPartCnt);
					// Extract the parameters from the message part
					String prtContent = prt.getContent().toString();

					Map<String, String> procEnv = procBuilder.environment();
					procEnv.put("XQ_SERVICE_NAME", params.getParameter(
							XQConstants.PARAM_SERVICE_NAME,
							XQConstants.PARAM_STRING));
					procEnv.put("XQ_CONTAINER_NAME", params.getParameter(
							XQConstants.PARAM_CONTAINER_NAME,
							XQConstants.PARAM_STRING));
					procEnv.put("XQ_DOMAIN_NAME", params.getParameter(
							XQConstants.PARAM_DOMAIN_NAME,
							XQConstants.PARAM_STRING));
					procEnv.put("XQ_ESBCONTAINER_NAME", params.getParameter(
							XQConstants.PARAM_XQ_CONTAINER_NAME,
							XQConstants.PARAM_STRING));
					XQProcessContext processContext = ctx.getProcessContext();
					if (processContext != null) {
						procEnv
								.put("XQ_PROCESS_NAME", processContext
										.getName());
						procEnv.put("XQ_PROCESS_STEP_NAME", processContext
								.getStepName());
						procEnv.put("XQ_PROCESS_TOP_PROCESS_NAME",
								processContext.getTopProcessName());
					}

					Iterator hn = msg.getHeaderNames();
					while (hn.hasNext()) {
						// hn.next();
						String propertyName = (String) hn.next();
						Object propertyValue = "";

						if (msg.getHeaderValue(propertyName) != null) {
							propertyValue = msg.getHeaderValue(propertyName);
						} else {
							propertyValue = "";
						}
						procEnv.put("MSG_HEADER_" + propertyName, propertyValue
								.toString());
					}
					hn = prt.getHeader().getKeys();
					proc = procBuilder.start();
					while (hn.hasNext()) {
						hn.next();
						String propertyName = (String) hn.next();
						Object propertyValue = "";

						if (prt.getHeader().getValue(propertyName) != null) {
							propertyValue = prt.getHeader().getValue(
									propertyName);
						} else {
							propertyValue = "";
						}
						procEnv.put("PART_HEADER_" + propertyName,
								propertyValue.toString());
					}

					OutputStreamWriter procInput = new OutputStreamWriter(proc
							.getOutputStream());
					BufferedReader procOut = new BufferedReader(
							new InputStreamReader(proc.getInputStream()));
					procInput.write(prtContent);
					procInput.flush();
					prtContent = "";

					Integer exitCode = null;
					while (exitCode == null) {
						while (procOut.ready()) {
							prtContent = prtContent + procOut.readLine() + "\n";
						}
						try {
							int exitCodeNumber = proc.exitValue();
							exitCode = new Integer(exitCodeNumber);
						} catch (NullPointerException npe) {
							m_xqLog.logInformation(m_logPrefix
									+ npe.getMessage()
									+ ": processo encerrado.");
							exitCode = new Integer(-1);
						} catch (IllegalThreadStateException itse) {
							m_xqLog.logDebug(m_logPrefix + itse.getMessage());
							try {
								Thread.currentThread().sleep(1000);
							} catch (InterruptedException e) {
								m_xqLog.logDebug(m_logPrefix + e.getMessage());
								exitCode = new Integer(-2);
							}
						}
					}
					m_xqLog.logDebug(m_logPrefix + prtContent);
					m_xqLog.logDebug(m_logPrefix + " Process exit code: "
							+ exitCode.toString());
					prt.setContent(prtContent, "text/plain");
					prt.getHeader().setValue("ExitValue", exitCode.toString());
					msg.replacePart(prt, iPartCnt);

				} catch (XQMessageException me) {
					throw new XQServiceException(
							"Exception accessing XQMessage: " + me.getMessage(),
							me);
				} catch (IOException e) {
					throw new XQServiceException(
							"Exception IOException : " + e.getMessage());
				}

				// Pass message onto the outbox.
				Iterator addressList = env.getAddresses();
				if (addressList.hasNext()) {
					// Add the message to the Outbox
					ctx.addOutgoing(env);
				}
			}
		}
		m_xqLog.logDebug(m_logPrefix + "Service processed...");
	}

	/**
	 * Clean up and get ready to destroy the service.
	 * 
	 * <p>
	 * This method implement a required XQService method.
	 */
	public void destroy() {
		m_xqLog.logInformation(m_logPrefix + "Destroying...");
		m_xqLog.logInformation(m_logPrefix + "Destroyed...");
	}

	/**
	 * Called by the container on container start.
	 * 
	 * <p>
	 * This method implement a required XQServiceEx method.
	 */
	public void start() {
		m_xqLog.logInformation(m_logPrefix + "Starting...");
		m_xqLog.logInformation(m_logPrefix + "Started...");
	}

	/**
	 * Called by the container on container stop.
	 * 
	 * <p>
	 * This method implement a required XQServiceEx method.
	 */
	public void stop() {
		m_xqLog.logInformation(m_logPrefix + "Stopping...");
		if (proc != null) {
			proc.destroy();
			proc = null;
		}
		m_xqLog.logInformation(m_logPrefix + "Stopped...");
	}

	/**
	 * Clean up and get ready to destroy the service.
	 * 
	 */
	protected void setLogPrefix(XQParameters params) {
		String serviceName = params.getParameter(
				XQConstants.PARAM_SERVICE_NAME, XQConstants.PARAM_STRING);
		m_logPrefix = "[ " + serviceName + " ]";
	}

	/**
	 * Provide access to the service implemented version.
	 * 
	 */
	protected String getVersion() {
		return s_major + "." + s_minor + ". build " + s_buildNumber;
	}

	/**
	 * Writes a standard service startup message to the log.
	 */
	protected void writeStartupMessage(XQParameters params) {
		final StringBuffer buffer = new StringBuffer();
		String serviceTypeName = params.getParameter(
				XQConstants.SERVICE_PARAM_SERVICE_TYPE,
				XQConstants.PARAM_STRING);
		buffer.append("\n\n");
		buffer.append("\t\t " + serviceTypeName + "\n ");
		buffer.append("\t\t Version ");
		buffer.append(" " + getVersion());
		buffer.append("\n");
		m_xqLog.logInformation(buffer.toString());
	}

	/**
	 * Writes parameters to log.
	 */
	protected void writeParameters(XQParameters params) {

		final Map map = params.getAllInfo();
		final Iterator iter = map.values().iterator();

		while (iter.hasNext()) {
			final XQParameterInfo info = (XQParameterInfo) iter.next();

			if (info.getType() == XQConstants.PARAM_XML) {
				m_xqLog.logInformation(m_logPrefix + "Parameter Name =  "
						+ info.getName());
			} else if (info.getType() == XQConstants.PARAM_STRING) {
				m_xqLog.logInformation(m_logPrefix + "Parameter Name = "
						+ info.getName());
			}

			if (info.getRef() != null) {
				m_xqLog.logInformation(m_logPrefix + "Parameter Reference = "
						+ info.getRef());

				// If this is too verbose
				// /then a simple change from logInformation to logDebug
				// will ensure file content is not displayed
				// unless the logging level is set to debug for the ESB
				// Container.
				m_xqLog.logInformation(m_logPrefix
						+ "----Parameter Value Start--------");
				m_xqLog.logInformation("\n" + info.getValue() + "\n");
				m_xqLog.logInformation(m_logPrefix
						+ "----Parameter Value End--------");
			} else {
				m_xqLog.logInformation(m_logPrefix + "Parameter Value = "
						+ info.getValue());
			}
		}
	}
}