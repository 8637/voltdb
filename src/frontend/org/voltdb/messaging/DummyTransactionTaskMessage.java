/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;

/**
 * Message from a client interface to an initiator, instructing the
 * site to begin executing a stored procedure, coordinating other
 * execution sites if needed.
 *
 */
public class DummyTransactionTaskMessage extends TransactionInfoBaseMessage
{
    public DummyTransactionTaskMessage()
    {
        super();
        m_isReadOnly = true;
        m_isForReplay = false;
    }

    public DummyTransactionTaskMessage (long initiatorHSId, long coordinatorHSId, long txnId, long uniqueId) {
        super(initiatorHSId, coordinatorHSId, txnId, uniqueId, true, false);
        m_isReadOnly = true;
        m_isForReplay = false;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.IV2_DUMP_SYNC_TASK_ID);
        super.flattenToBuffer(buf);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException {
        super.initFromBuffer(buf);
        assert(buf.capacity() == buf.position());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("DummyTaskMessage (FROM ");
        sb.append(CoreUtils.hsIdToString(m_sourceHSId));
        return sb.toString();
    }
}
