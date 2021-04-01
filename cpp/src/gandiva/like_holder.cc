// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <regex>
#include "gandiva/node.h"
#include "gandiva/like_holder.h"

namespace gandiva {

RE2 RegexpMatchesHolder::starts_with_regex_(R"(\^([\w\s]+)(\.\*)?)");
RE2 RegexpMatchesHolder::ends_with_regex_(R"((\.\*)?([\w\s]+)\$)");
RE2 RegexpMatchesHolder::is_substr_regex_(R"((\w|\s)*)");

// Short-circuit pattern matches for the three common sub cases :
// - starts_with, ends_with, and contains.
const FunctionNode RegexpMatchesHolder::TryOptimize(const FunctionNode& node) {
  std::shared_ptr<RegexpMatchesHolder> holder;
  auto status = Make(node, &holder);
  if (status.ok()) {
    std::string& pattern = holder->pattern_;
    auto literal_type = node.children().at(1)->return_type();
    std::string substr;
    if (RE2::FullMatch(pattern, starts_with_regex_, &substr)) {
      auto prefix_node =
          std::make_shared<LiteralNode>(literal_type, LiteralHolder(substr), false);
      return FunctionNode("starts_with", {node.children().at(0), prefix_node},
                          node.return_type());
    } else if (RE2::FullMatch(pattern, ends_with_regex_, (void*)NULL, &substr)) {
      auto suffix_node =
          std::make_shared<LiteralNode>(literal_type, LiteralHolder(substr), false);
      return FunctionNode("ends_with", {node.children().at(0), suffix_node},
                          node.return_type());
    } else if (RE2::FullMatch(pattern, is_substr_regex_)) {
      auto substr_node =
          std::make_shared<LiteralNode>(literal_type, LiteralHolder(pattern), false);
      return FunctionNode("is_substr", {node.children().at(0), substr_node},
                          node.return_type());
    }
  }

  // Could not optimize, return original node.
  return node;
}

static bool IsArrowStringLiteral(arrow::Type::type type) {
  return type == arrow::Type::STRING || type == arrow::Type::BINARY;
}

Status RegexpMatchesHolder::ValidateArguments(const FunctionNode& node) {
  ARROW_RETURN_IF(node.children().size() != 2,
                  Status::Invalid("'" + node.descriptor()->name() +
                                  "' function requires two parameters"));

  auto literal = dynamic_cast<LiteralNode*>(node.children().at(1).get());
  ARROW_RETURN_IF(
      literal == nullptr,
      Status::Invalid("'" + node.descriptor()->name() +
                      "' function requires a literal as the second parameter"));

  auto literal_type = literal->return_type()->id();
  ARROW_RETURN_IF(
      !IsArrowStringLiteral(literal_type),
      Status::Invalid("'" + node.descriptor()->name() +
                      "' function requires a string literal as the second parameter"));

  return Status::OK();
}

Result<std::string> RegexpMatchesHolder::GetPattern(const FunctionNode& node) {
  ARROW_RETURN_NOT_OK(ValidateArguments(node));
  auto literal = dynamic_cast<LiteralNode*>(node.children().at(1).get());
  auto pattern = arrow::util::get<std::string>(literal->holder());
  return pattern;
}

Status RegexpMatchesHolder::Make(const std::string& pcre_pattern,
                       std::shared_ptr<RegexpMatchesHolder>* holder) {
  auto lholder =
      std::shared_ptr<RegexpMatchesHolder>(new RegexpMatchesHolder(pcre_pattern));
  ARROW_RETURN_IF(!lholder->regex_.ok(),
                  Status::Invalid("Building RE2 pattern '", pcre_pattern, "' failed"));

  *holder = lholder;
  return Status::OK();
}

Status RegexpMatchesHolder::Make(const FunctionNode& node,
                                 std::shared_ptr<RegexpMatchesHolder>* holder) {
  ARROW_ASSIGN_OR_RAISE(std::string pattern, GetPattern(node));

  return Make(pattern, holder);
}

Status SQLLikeHolder::Make(const std::string& sql_pattern,
                           std::shared_ptr<SQLLikeHolder>* holder) {
  std::string pcre_pattern;
  ARROW_RETURN_NOT_OK(RegexUtil::SqlLikePatternToPcre(sql_pattern, pcre_pattern));

  std::shared_ptr<RegexpMatchesHolder> base_holder;
  RegexpMatchesHolder::Make(pcre_pattern, &base_holder);

  *holder = std::static_pointer_cast<SQLLikeHolder>(base_holder);;
  return Status::OK();
}

Status SQLLikeHolder::Make(const FunctionNode& node,
                                 std::shared_ptr<SQLLikeHolder>* holder) {
  ARROW_ASSIGN_OR_RAISE(std::string pattern, GetPattern(node));

  return Make(pattern, holder);
}


} // namespace gandiva
