package org.apache.hadoop.coordination.zk.bench;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.coordination.ConsensusProposal;

/**
 * Used for loader threads registration.
 */
public class RegisterProposal extends ConsensusProposal<LoadTool.LoadGenerator, Void> implements Serializable {

  private final int requestId;

  public RegisterProposal(Serializable proposerNodeId, int requestId) {
    super(proposerNodeId);
    this.requestId = requestId;
  }

  @Override
  public Void execute(LoadTool.LoadGenerator callBackObject) throws IOException {
    callBackObject.register(getProposerNodeId(), requestId);
    return null;
  }
}
