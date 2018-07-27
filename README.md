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

            if (!remainTE)
                shortestPathBuilder.Option(new TETableClear());

            shortestPathBuilder.Option(new TEViewClear());

            if (!remainTE)
                shortestPathBuilder.Option(new NormalImporter(dataSet));

            shortestPathBuilder
                    // Build TE TABLE
                    .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
                    // TA Clear
                    .Option(new PrepareBDThread(true))
                    // BD Thread Table Prepare
                    .Runner(new BiDirectionalThread(100, BiDirectionalThread.THREAD_USE_RB));
            shortestPathBuilder.prepare();
            return shortestPathBuilder.run(source, target);
        }

```
