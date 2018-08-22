package kr.co.kdelab.postgre.routing.redfish.algo.impl.bidirectional;

import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.FrontierResult;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.MergeResult;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface SingleThreadImpl {
    default MergeResult singleFEM(PreparedStatement ERMergeOP, int iteration, int direction) throws SQLException {
        MergeResult mergeResult = new MergeResult(0, 0);


        FrontierResult frontierResult = getFrontier(direction);
        if (frontierResult == null) {
            mergeResult.affected = Integer.MAX_VALUE;
            return mergeResult;
        }

        setVisited(direction, frontierResult.getNodeId());

        ERMergeOP.setInt(1, frontierResult.getCost());
        ERMergeOP.setInt(2, frontierResult.getNodeId());
        ERMergeOP.setInt(3, iteration + 1);
        ERMergeOP.setInt(4, frontierResult.getNodeId());

        mergeResult.affected = ERMergeOP.executeUpdate();
        mergeResult.minCost = frontierResult.getCost();

        return mergeResult;

    }

    FrontierResult getFrontier(int direction) throws SQLException;

    void setVisited(int direction, int nodeId) throws SQLException;
}
