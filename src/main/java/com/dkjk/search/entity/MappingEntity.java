package com.dkjk.search.entity;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author liubaohua
 * @date 2019-10-31 15:05
 */
@Data
public class MappingEntity {

    private String dynamic;

    private List<Map<String,FieldEntity>> properties;

}
