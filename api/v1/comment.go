package v1

import (
	"database/sql"
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/ravshancoder/blog/api/models"
	"github.com/ravshancoder/blog/storage/repo"
)

// @Security ApiKeyAuth
// @Router /comments [post]
// @Summary Create a comment
// @Description Create a comment
// @Tags comment
// @Accept json
// @Produce json
// @Param comment body models.CreateCommentRequest true "comment"
// @Success 201 {object} models.Comment
// @Failure 500 {object} models.ErrorResponse
func (h *handlerV1) CreateComment(c *gin.Context) {
	var (
		req models.CreateCommentRequest
	)
	
	err := c.ShouldBindJSON(&req)
	if err != nil {
		c.JSON(http.StatusBadRequest, errorResponse(err))
		return
	}

	payload, err := h.GetAuthPayload(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, errorResponse(err))
		return
	}
	
	resp, err := h.storage.Comment().Create(&repo.Comment{
		Description: req.Description,
		PostID:      req.PostID,
		UserID:      payload.UserID,
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, errorResponse(err))
		return
	}

	comment := parseCommentModel(resp)
	c.JSON(http.StatusCreated, comment)
}

// @Router /comments [get]
// @Summary Get all comments
// @Description Get all comments
// @Tags comment
// @Accept json
// @Produce json
// @Param filter query models.GetAllCommentsParams false "Filter"
// @Success 200 {object} models.GetAllCommentsResponse
// @Failure 500 {object} models.ErrorResponse
func (h *handlerV1) GetAllComments(c *gin.Context) {
	req, err := validateGetAllCommentsParams(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, errorResponse(err))
		return
	}

	result, err := h.storage.Comment().GetAll(&repo.GetAllCommentsParams{
		Page:   req.Page,
		Limit:  req.Limit,
		UserID: req.UserID,
		PostID: req.PostID,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, errorResponse(err))
		return
	}

	c.JSON(http.StatusOK, getCommentsResponse(result))
}

// @Router /comments/{id} [put]
// @Summary Update a comment
// @Description Update a comment
// @Tags comment
// @Accept json
// @Produce json
// @Param id path int true "ID"
// @Param comment body models.CreateCommentRequest true "Comment"
// @Success 200 {object} models.Comment
// @Failure 500 {object} models.ErrorResponse
func (h *handlerV1) UpdateComment(c *gin.Context) {
	var req repo.Comment

	err := c.ShouldBindJSON(&req)
	if err != nil {
		c.JSON(http.StatusBadRequest, errorResponse(err))
		return
	}

	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, errorResponse(err))
		return
	}
	
	req.ID = int64(id)
	
	updated, err := h.storage.Comment().Update(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, errorResponse(err))
		return
	}
	
	c.JSON(http.StatusCreated, parseCommentModel(updated))
}

// @Security ApiKeyAuth
// @Router /comments/{id} [delete]
// @Summary Delete a comment
// @Description Delete a comment
// @Tags comment
// @Accept json
// @Produce json
// @Param id path int true "ID"
// @Success 200 {object} models.ResponseOK
// @Failure 400 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
func (h *handlerV1) DeleteComment(c *gin.Context) {
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, errorResponse(err))
		return
	}

	err = h.storage.Comment().Delete(int64(id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			c.JSON(http.StatusNotFound, errorResponse(err))
			return
		}
		c.JSON(http.StatusInternalServerError, errorResponse(err))
		return
	}
	
	c.JSON(http.StatusOK, models.ResponseOK{
		Message: "Successfully deleted",
	})
}



func validateGetAllCommentsParams(c *gin.Context) (*models.GetAllCommentsParams, error) {
	var (
		limit          int = 10
		page           int = 1
		err            error
		userID, postID int
	)

	if c.Query("limit") != "" {
		limit, err = strconv.Atoi(c.Query("limit"))
		if err != nil {
			return nil, err
		}
	}

	if c.Query("page") != "" {
		page, err = strconv.Atoi(c.Query("page"))
		if err != nil {
			return nil, err
		}
	}

	if c.Query("user_id") != "" {
		userID, err = strconv.Atoi(c.Query("user_id"))
		if err != nil {
			return nil, err
		}
	}

	if c.Query("post_id") != "" {
		postID, err = strconv.Atoi(c.Query("post_id"))
		if err != nil {
			return nil, err
		}
	}

	return &models.GetAllCommentsParams{
		Limit:  int32(limit),
		Page:   int32(page),
		UserID: int64(userID),
		PostID: int64(postID),
	}, nil
}

func getCommentsResponse(data *repo.GetAllCommentsResult) *models.GetAllCommentsResponse {
	response := models.GetAllCommentsResponse{
		Comments: make([]*models.Comment, 0),
		Count:    data.Count,
	}

	for _, comment := range data.Comments {
		p := parseCommentModel(comment)
		response.Comments = append(response.Comments, &p)
	}

	return &response
}

func parseCommentModel(comment *repo.Comment) models.Comment {
	return models.Comment{
		ID:          comment.ID,
		UserID:      comment.UserID,
		PostID:      comment.PostID,
		Description: comment.Description,
		CreatedAt:   comment.CreatedAt,
		UpdatedAt:   comment.UpdatedAt,
		User: &models.CommentUser{
			ID:              comment.UserID,
			FirstName:       comment.User.FirstName,
			LastName:        comment.User.LastName,
			Email:           comment.User.Email,
			ProfileImageUrl: comment.User.ProfileImageUrl,
		},
	}
}