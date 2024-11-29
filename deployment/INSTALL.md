## Steps to Install Terraform

1. Follow the instructions on [this](https://developer.hashicorp.com/terraform/install) page and install terraform according to OS.
2. [OPTIONAL] Create an alias for terraform in ~/.bashrc file.

    ```shell
    alias tf='terraform'
3. [OPTIONAL] To enable autocomplete to work for both `terraform` and `tf` commands, run the following command.

    ```shell
    printf  "complete -C /usr/bin/terraform terraform\ncomplete -C /usr/bin/terraform tf"  |  sudo tee -a /etc/bash_completion.d/terraform
4. Test your installation by running `tf --version` command.
