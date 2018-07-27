# Shortest Path on PostgreSQL

기본 아이디어는 FEM Framework를 확장합니다.
- https://www.researchgate.net/publication/259744306_Shortest_Path_Computing_in_Relational_DBMSs

제공하는 알고리즘 리스트
- bi-directional restrictive BFS ( Jun. Gao )
- bi-directional FEM ( Jun. Gao )
- bi-directional FEM Thread ( Seo, Kwak )
- bi-directional FEM Thread, TA Index ( Seo, Kwak )

### 사용 예제
Public
```java
var jDBConnectionInfo = new JDBConnectionInfo("DBURL", "USERID", "USERPW", "SCHEMA");
// JDBC Connection용 객체
// ( var 문법은 JDK 10 이상부터 지원함. )

```

1. bi-directional FEM Thread
```java
try (ShortestPathBuilder shortestPathBuilder = new ShortestPathBuilder().JDBC(jdbConnectionInfo)) {
        shortestPathBuilder.Option(new TETableClear()); // DataSet Table-1
        shortestPathBuilder.Option(new TEViewClear()); // DataSet Table Clear-2
        shortestPathBuilder.Option(new NormalImporter(dataSet)); // Create DataSet Table

        shortestPathBuilder
                .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
                // Algorithm data table
                .Option(new PrepareBDThread(true))
                .Runner(new BiDirectionalThread(100, BiDirectionalThread.THREAD_NORMAL));
                // THREAD_NORMAL, THREAD_USE_RB, THREAD_USE_TA_INDEX, THREAD_USE_RB_TA_INDEX
        shortestPathBuilder.prepare();
        System.out.println(shortestPathBuilder.run(source, target));
}

```
2. bi-directional restrictive BFS
```java
try (ShortestPathBuilder shortestPathBuilder = new ShortestPathBuilder().JDBC(jdbConnectionInfo)) {
        shortestPathBuilder.Option(new TETableClear()); // DataSet Table-1
        shortestPathBuilder.Option(new TEViewClear()); // DataSet Table Clear-2
        shortestPathBuilder.Option(new PartitioningImporter(dataSet, pts, pv, true)); // Create DataSet Table
        // bi-directional restrictive BFS must use PartitioningImporter

        shortestPathBuilder
                .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
                // Algorithm data table
                .Option(new PrepareBiRbfs())
                .Runner(new BiRbfs(pts, pv));
                // support ReachedBiRbfs
        shortestPathBuilder.prepare();
        System.out.println(shortestPathBuilder.run(source, target));
}
```
3. DataSet Table Prepare
```java
try (ShortestPathBuilder shortestPathBuilder = new ShortestPathBuilder().JDBC(jdbConnectionInfo)) {
        shortestPathBuilder.Option(new TETableClear())
                .Option(new TEViewClear())
                .Option(new NormalImporter(dataSet))
        shortestPathBuilder.prepare();
}
```
4-1. Rechability Method - Join
```java
// after prepared DataSet Table ( FULL TE )
try (JoinCalculator calculator = new JoinCalculator(jDBConnectionInfo)) {
        if (calculator.calc(13576, 245646))
                System.out.println("Successed");
        else
                throw new IOException("Failed");
} catch (SQLException | IOException | InterruptedException e) {
        System.out.println("Error");
}

```
4-2. Rechability Method - Version_1
```java
// after prepared DataSet Table ( FULL TE )
// Submit1Calculator ( JDBConnectionInfo, doubleUndirected # USA_ROAD-TRUE )
try (Submit1Calculator calculator = new Submit1Calculator(jDBConnectionInfo, true)) {
        if (calculator.calc(13576, 245646))
                System.out.println("Successed");
        else
                throw new IOException("Failed");
} catch (SQLException | IOException | InterruptedException e) {
        System.out.println("Error");
}
```
! Delay - ~28988 ms

USE RB = 7080 ms

NORMAL = 58754 ms 
