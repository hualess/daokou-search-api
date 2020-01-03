package com.dkjk.search.entity;

import lombok.Data;

/**
 * @author liubaohua
 * @date 2019-10-31 15:08
 */
@Data
public class FieldEntity {

      private String type;

      private boolean index;

      private String analyzer;
}
