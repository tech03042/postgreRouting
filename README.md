# Shortest Path on PostgreSQL

기본 아이디어는 FEM Framework를 확장합니다.
- https://www.researchgate.net/publication/259744306_Shortest_Path_Computing_in_Relational_DBMSs

제공하는 알고리즘 리스트
- bi-directional restrictive BFS ( Jun. Gao )
- bi-directional FEM ( Jun. Gao )
- bi-directional FEM Thread ( Seo, Kwak )
- bi-directional FEM Thread, TA Index ( Seo, Kwak )

```java
try (ShortestPathBuilder shortestPathBuilder = new ShortestPathBuilder().JDBC(jdbConnectionInfo)) {
        shortestPathBuilder.Option(new TETableClear()); // DataSet Table-1
        shortestPathBuilder.Option(new TEViewClear()); // DataSet Table Clear-2
        shortestPathBuilder.Option(new NormalImporter(dataSet)); // Create DataSet Table

        shortestPathBuilder
                .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
                // Algorithm data table
                .Option(new PrepareBDThread(true))
                .Runner(new BiDirectionalThread(100, BiDirectionalThread.THREAD_USE_RB));
        shortestPathBuilder.prepare();
        System.out.println(shortestPathBuilder.run(source, target));
}

```
