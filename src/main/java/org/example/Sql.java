package org.example;

import java.lang.reflect.InvocationTargetException;
import java.time.LocalDateTime;
import java.util.*;

public class Sql {
    private final StringBuilder sqlBuilder;
    private final List<Object> parameters;

    final SimpleDb simpleDb = new SimpleDb("localhost", "root",
            "010416aa", "simpledb");

    public Sql(SimpleDb simpleDb) {
        sqlBuilder = new StringBuilder();
        parameters = new ArrayList<>();
    }

    public Sql append(String sqlPart) {
        sqlBuilder.append(" ").append(sqlPart);
        return this;
    }

    public Sql append(String sqlPart, Object... params) {
        sqlBuilder.append(" ").append(sqlPart);
        for (Object param : params) {
            parameters.add(param);
        }
        return this;
    }

    public Sql appendIn(String sqlPart, Object... ids) {
        sqlBuilder.append(" ").append(sqlPart.replace("?",
                String.join(", ", Collections.nCopies(ids.length, "?"))));

        parameters.addAll(Arrays.asList(ids));
        return this;
    }

    public long insert() {
        String sql = sqlBuilder.toString();
        try (simpleDb) {
            return simpleDb.update(sql, parameters.toArray());
        }
    }

    public int update() {
        String sql = sqlBuilder.toString();
        try (simpleDb) {
            return (int) simpleDb.update(sql, parameters.toArray());
        }
    }

    public int delete() {
        String sql = sqlBuilder.toString();
        try (simpleDb) {
            return (int) simpleDb.update(sql, parameters.toArray());
        }
    }

    public List<Map<String, Object>> selectRows() {
        String sql = sqlBuilder.toString();
        try (simpleDb) {
            List<Article> articles = simpleDb.selectRows(sql, parameters.toArray(), Article.class);

            List<Map<String, Object>> result = new ArrayList<>();
            for (Article article : articles) {
                Map<String, Object> articleMap = new HashMap<>();
                articleMap.put("id", article.getId());
                articleMap.put("title", article.getTitle());
                articleMap.put("body", article.getBody());
                articleMap.put("createdDate", article.getCreatedDate());
                articleMap.put("modifiedDate", article.getModifiedDate());
                articleMap.put("isBlind", article.isBlind());
                result.add(articleMap);
            }

            return result;
        }
    }

    public List<Article> selectRows(Class<Article> clazz) {
        String sql = sqlBuilder.toString();
        try (simpleDb) {
            List<Article> articles = simpleDb.selectRows(sql, parameters.toArray(), clazz);
            return articles;
        }
    }

    public Map<String, Object> selectRow() {
        String sql = sqlBuilder.toString();
        try (simpleDb) {
            return simpleDb.selectRow(sql, parameters.toArray());
        }
    }

    public <T> T selectRow(Class<T> clazz) {
        String sql = sqlBuilder.toString();
        try (simpleDb) {
            Map<String, Object> row = simpleDb.selectRow(sql, parameters.toArray());
            if (row != null) {
                T instance = clazz.getDeclaredConstructor().newInstance();

                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    String setterName = "set" + entry.getKey().substring(0, 1).toUpperCase()
                            + entry.getKey().substring(1);
                    try {
                        var setter = clazz.getMethod(setterName, entry.getValue().getClass());
                        setter.invoke(instance, entry.getValue());
                    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
                return instance;
            }
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException |
                 InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    public LocalDateTime selectDatetime() {
        String sql = sqlBuilder.toString();
        try (simpleDb) {
            return simpleDb.selectDatetime(sql, parameters.toArray());
        }
    }

    public Long selectLong() {
        String sql = sqlBuilder.toString();
        try (simpleDb) {
            return simpleDb.selectLong(sql, parameters.toArray());
        }
    }

    public String selectString() {
        String sql = sqlBuilder.toString();
        try (simpleDb) {
            return simpleDb.selectString(sql, parameters.toArray());
        }
    }

    public Boolean selectBoolean() {
        String sql = sqlBuilder.toString();
        try (simpleDb) {
            return simpleDb.selectBoolean(sql, parameters.toArray());
        }
    }

    public List<Long> selectLongs() {
        String sql = sqlBuilder.toString();
        try (simpleDb) {
            List<Map<String, Object>> rows = simpleDb.selectRows(sql, parameters.toArray());
            List<Long> longs = new ArrayList<>();
            for (Map<String, Object> row : rows) {
                longs.add((Long) row.values().iterator().next());
            }
            return longs;
        }
    }
}