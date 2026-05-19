variable "project_name" {
  type    = string
  default = "ecommerce-platform"
}

variable "aws_region" {
  type    = string
  default = "eu-west-1"
}

variable "subnet_ids" {
  type    = list(string)
  default = ["subnet-abc12345", "subnet-def12345", "subnet-ghi12345"]
}

variable "security_group_id" {
  type    = string
  default = "sg-12345678"
}

variable "snowflake_account" {
  type    = string
  default = "xy12345.west-europe.azure"
}

variable "snowflake_user" {
  type    = string
  default = "DATA_ENGINEER_USER"
}

variable "snowflake_password" {
  type      = string
  sensitive = true
}
