# Shortest Path on PostgreSQL

기본 아이디어는 FEM Framework를 확장합니다.
- https://www.researchgate.net/publication/259744306_Shortest_Path_Computing_in_Relational_DBMSs

제공하는 알고리즘 리스트
- bi-directional restrictive BFS ( Jun. Gao ) Seo, Kwak implemented.
- bi-directional FEM ( Jun. Gao ) Seo, Kwak implemented.
- bi-directional FEM Thread ( Seo, Kwak ) Seo, Kwak implemented.
- bi-directional FEM Thread, TA Index ( Seo, Kwak ) Seo, Kwak implemented.
- bi-directional restrictive BFS Seo implemented.

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
        shortestPathBuilder.Option(new PartitioningImporter(dataSet, pts, pv)); // Create DataSet Table
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
                .Option(new NormalImporter(dataSet)) // PartitioningImporter(dataSet, pts, pv)
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
5-1. Seo, implemented. RBFS
- prepareDataSet
```java
try (ShortestPathBuilder shortestPathBuilder = new ShortestPathBuilder().JDBC(jdbConnectionInfo)) {
    shortestPathBuilder.Option(new TETableClear())
            .Option(new TEViewClear())
            .Option(new CustomImporter(dataSet, pts, pv));
    shortestPathBuilder.prepare();
}
```
- shortestPath
```java
try (ShortestPathBuilder shortestPathBuilder = new ShortestPathBuilder().JDBC(jdbConnectionInfo)) {
    shortestPathBuilder
            .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
            // TA Clear
            .Option(new ERClear())
            .Option(new PrepareSeoRBFS())
            // BD Thread Table Prepare
            .Runner(new SeoRBFSRunnerReached(pts, pv));
    shortestPathBuilder.prepare();
    return shortestPathBuilder.run(source, target);
}
```
##### History
- 잘못된 지우기 쿼리 수정 ( 180728 )
- pgPL/SQL -> 개별적인 쿼리로 수정 ( 180728 )
- BDThread(NY, 13576 => 245646) 28988 ms(R)+7080 ms (R), 58754 ms (N)
- PartitioningApplier TE 테이블을 기본으로 생성하도록 변경


##### 까먹는것 메모용
- Submit1Calculator는 가장 처음에 했던 가지치기를 개선한것.
    - 대상이 yago(DAG)일 경우에는 boolean 매개변수 false 전달 필요 