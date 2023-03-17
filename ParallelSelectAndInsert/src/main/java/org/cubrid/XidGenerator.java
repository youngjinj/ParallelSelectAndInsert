package org.cubrid;

import java.nio.ByteBuffer;
import java.util.UUID;

import javax.transaction.xa.Xid;

import cubrid.jdbc.driver.CUBRIDXid;

public class XidGenerator {
	private final int formatId;
	private final byte[] globalTransactionId;
	private int branchQualifierCounter;

	public XidGenerator() {
		UUID uuid = UUID.randomUUID();
		this.formatId = 1;
		this.globalTransactionId = ByteBuffer.allocate(Long.BYTES).putLong(uuid.getMostSignificantBits()).array();
		this.branchQualifierCounter = 0;
	}

	public Xid generateXid() {
		int nextBranchQualifier = getNextBranchQualifier();
		return new CUBRIDXid(this.formatId, this.globalTransactionId,
				ByteBuffer.allocate(Long.BYTES).putLong(nextBranchQualifier).array());
	}

	private int getNextBranchQualifier() {
		return this.branchQualifierCounter++;
	}
}
